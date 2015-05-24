package net.openhft.chronicle.engine2.map;

import com.sun.nio.file.SensitivityWatchEventModifier;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.FactoryContext;
import net.openhft.chronicle.engine2.api.Subscriber;
import net.openhft.chronicle.engine2.api.TopicSubscriber;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A {@link Map} implementation that stores each entry as a file in a
 * directory. The <code>key</code> is the file name and the <code>value</code>
 * is the contents of the file.
 * <p>
 * The class is effectively an abstraction over a directory in the file system.
 * Therefore when the underlying files are changed an event will be fired to those
 * registered for notifications.
 * <p>
 * <p>
 * Updates will be fired every time the file is saved but will be suppressed
 * if the value has not changed.  To avoid temporary files (e.g. if edited in vi)
 * being included in the map, any file starting with a '.' will be ignored.
 * <p>
 * Note the {@link WatchService} is extremely OS dependant.  Mas OSX registers
 * very few events if they are done quickly and there is a significant delay
 * between the event and the event being triggered.
 */
public class FilePerKeyValueStore<V extends Marshallable> implements SubscriptionKeyValueStore<String, V>, Closeable {
    private final Path dirPath;
    //Use BytesStore so that it can be shared safely between threads
    private final Map<File, FileRecord<V>> lastFileRecordMap = new ConcurrentHashMap<>();

    private final Thread fileFpmWatcher;
    private final Bytes<ByteBuffer> writingBytes = Bytes.elasticByteBuffer();
    private final Bytes<ByteBuffer> readingBytes = Bytes.elasticByteBuffer();
    private final Wire writingWire;
    private final Wire readingWire;
    private final SubscriptionKVSCollection<String, V> subscriptions = new SubscriptionKVSCollection<>(this);
    private final Constructor<V> vConstructor;
    private volatile boolean closed = false;
    private Asset asset;

    public FilePerKeyValueStore(FactoryContext context) throws IORuntimeException {
        try {
            assert context.type() == String.class;
            this.vConstructor = context.type2().getConstructor();
            vConstructor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }

        String first = context.basePath();
        String dirName = first == null ? context.name() : first + "/" + context.name();
        this.dirPath = Paths.get(dirName);
        Function<Bytes, Wire> bytesToWire = context.writeType();
        writingWire = bytesToWire.apply(writingBytes);
        readingWire = bytesToWire.apply(readingBytes);
        WatchService watcher;
        try {
            Files.createDirectories(dirPath);
            watcher = FileSystems.getDefault().newWatchService();
            dirPath.register(watcher, new WatchEvent.Kind[]{
                            StandardWatchEventKinds.ENTRY_CREATE,
                            StandardWatchEventKinds.ENTRY_DELETE,
                            StandardWatchEventKinds.ENTRY_MODIFY},
                    SensitivityWatchEventModifier.HIGH
            );
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }

        fileFpmWatcher = new Thread(new FPMWatcher(watcher), dirName + "-watcher");
        fileFpmWatcher.setDaemon(true);
        fileFpmWatcher.start();
    }

    @Override
    public long size() {
        return getFiles().count();
    }

    @Override
    public V getUsing(String key, V value) {
        Path path = dirPath.resolve(key);
        return getFileContents(path, value);
    }

    @Override
    public void keysFor(int segment, Consumer<String> stringConsumer) {
        getFiles().map(p -> p.getFileName().toString()).forEach(stringConsumer);
    }

    @Override
    public void entriesFor(int segment, Consumer<Entry<String, V>> kvConsumer) {
        getFiles().map(p -> Entry.of(p.getFileName().toString(), getFileContents(p, null)))
                .forEach(kvConsumer);
    }

    @Override
    public Iterator<Map.Entry<String, V>> entrySetIterator() {
        return getFiles()
                .map(p -> (Map.Entry<String, V>) new AbstractMap.SimpleEntry<>(p.getFileName().toString(), getFileContents(p, null)))
                .iterator();
    }

    @Override
    public void put(String key, V value) {
        if (closed) throw new IllegalStateException("closed");
        Path path = dirPath.resolve(key);
        FileRecord fr = lastFileRecordMap.get(path.toFile());
        writeToFile(path, value);
        if (fr != null) fr.valid = false;
    }

    @Override
    public V getAndPut(String key, V value) {
        if (closed) throw new IllegalStateException("closed");
        Path path = dirPath.resolve(key);
        FileRecord fr = lastFileRecordMap.get(path.toFile());
        V existingValue = getFileContents(path, value);
        writeToFile(path, value);
        if (fr != null) fr.valid = false;
        return existingValue;
    }

    @Override
    public V getAndRemove(String key) {
        if (closed) throw new IllegalStateException("closed");
        V existing = get(key);
        if (existing != null) {
            deleteFile(dirPath.resolve(key));
        }
        return existing;
    }

    @Override
    public void remove(String key) {
        if (closed) throw new IllegalStateException("closed");
        Path resolve = dirPath.resolve(key);
        if (resolve.toFile().isFile())
            deleteFile(resolve);
    }

    @Override
    public void clear() {
        AtomicInteger count = new AtomicInteger();
        Stream<Path> files = getFiles();
        files.forEach((path) -> {
            try {
                deleteFile(path);
            } catch (Exception e) {
                count.incrementAndGet();
                // ignored at afirst.
            }
        });
        if (count.intValue() > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().isInterrupted();
            }
            getFiles().forEach(this::deleteFile);
        }
    }

    private Stream<Path> getFiles() {
        try {
            return Files.walk(dirPath).filter(p -> !Files.isDirectory(p));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    V getFileContents(Path path, V using) {
        try {
            File file = path.toFile();
            FileRecord<V> lastFileRecord = lastFileRecordMap.get(file);
            if (lastFileRecord != null && lastFileRecord.valid
                    && file.lastModified() == lastFileRecord.timestamp) {
                return lastFileRecord.contents;
            }
            return getFileContentsFromDisk(path, using);
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    V getFileContentsFromDisk(Path path, V using) throws IOException {
        if (!Files.exists(path)) return null;
        File file = path.toFile();

        synchronized (readingBytes) {
            try (FileChannel fc = new FileInputStream(file).getChannel()) {
                readingBytes.ensureCapacity(fc.size());

                ByteBuffer dst = readingBytes.underlyingObject();
                dst.clear();

                fc.read(dst);

                readingBytes.position(0);
                readingBytes.limit(dst.position());
            }

            V v = using == null ? createValue() : using;
            v.readMarshallable(readingWire);
            return v;
        }
    }

    private V createValue() {
        try {
            return vConstructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new AssertionError(e);
        }
    }

    private void writeToFile(Path path, V value) {
        synchronized (writingBytes) {
            writingBytes.clear();
            value.writeMarshallable(writingWire);

            File file = path.toFile();
            File tmpFile = new File(file.getParentFile(), "." + file.getName());
            try (FileChannel fc = new FileOutputStream(tmpFile).getChannel()) {
                ByteBuffer byteBuffer = writingBytes.underlyingObject();
                byteBuffer.position(0);
                byteBuffer.limit((int) writingBytes.position());
                fc.write(byteBuffer);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            try {
                Files.move(tmpFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private void deleteFile(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void close() {
        closed = true;
        fileFpmWatcher.interrupt();
    }

    @Override
    public <E> void registerSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        subscriptions.registerSubscriber(eClass, subscriber, query);
    }

    @Override
    public <E> void registerSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber, String query) {
        subscriptions.registerSubscriber(eClass, subscriber, query);
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        subscriptions.unregisterSubscriber(eClass, subscriber, query);
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, TopicSubscriber<E> subscriber, String query) {
        subscriptions.unregisterSubscriber(eClass, subscriber, query);
    }

    @Override
    public void asset(Asset asset) {
        if (this.asset != null) throw new IllegalStateException();
        this.asset = asset;
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public void underlying(KeyValueStore underlying) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueStore underlying() {
        return null;
    }

    private class FPMWatcher implements Runnable {
        private final WatchService watcher;

        public FPMWatcher(WatchService watcher) {
            this.watcher = watcher;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    WatchKey key = null;
                    try {
                        key = processKey();
                    } catch (InterruptedException e) {
                        return;
                    } finally {
                        if (key != null) key.reset();
                    }
                }
            } catch (Throwable e) {
                if (!closed)
                    e.printStackTrace();
            }
        }

        @NotNull
        private WatchKey processKey() throws InterruptedException, IOException {
            WatchKey key = watcher.take();
            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();

                if (kind == StandardWatchEventKinds.OVERFLOW) {
                    // todo log a warning.
                    continue;
                }

                // get file name
                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path fileName = ev.context();
                String mapKey = fileName.toString();

                if (mapKey.startsWith(".")) {
                    //this avoids temporary files being added to the map
                    continue;
                }

                if (kind == StandardWatchEventKinds.ENTRY_CREATE || kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                    Path p = dirPath.resolve(fileName);
                    V mapVal = null;
                    try {
                        mapVal = getFileContentsFromDisk(p, null);
                    } catch (FileNotFoundException ignored) {
                    }
                    FileRecord<V> prev = lastFileRecordMap.put(p.toFile(), new FileRecord<>(p.toFile().lastModified(), mapVal));
                    subscriptions.notifyUpdate(p.toFile().getName(), prev == null ? null : prev.contents, mapVal);

                } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                    Path p = dirPath.resolve(fileName);

                    FileRecord<V> prev = lastFileRecordMap.remove(p.toFile());
                    V lastVal = prev == null ? null : prev.contents;
                    subscriptions.notifyRemoval(p.toFile().getName(), lastVal);

                }
            }
            return key;
        }
    }
}

