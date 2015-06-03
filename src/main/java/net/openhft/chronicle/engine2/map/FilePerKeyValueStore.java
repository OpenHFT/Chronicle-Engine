package net.openhft.chronicle.engine2.map;

import com.sun.nio.file.SensitivityWatchEventModifier;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.InvalidSubscriberException;
import net.openhft.chronicle.engine2.api.RequestContext;
import net.openhft.chronicle.engine2.api.SubscriptionConsumer;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.MapReplicationEvent;
import net.openhft.chronicle.engine2.api.map.StringBytesStoreKeyValueStore;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * A {@link Map} implementation that stores each entry as a file in a
 * directory. The <code>key</code> is the file fullName and the <code>value</code>
 * is the contents of the file.
 * <p>
 * The class is effectively an abstraction over a directory in the file system.
 * Therefore when the underlying files are changed an event will be fired to those
 * registered for notifications.
 * <p>
 * Updates will be fired every time the file is saved but will be suppressed
 * if the value has not changed.  To avoid temporary files (e.g. if edited in vi)
 * being included in the map, any file starting with a '.' will be ignored.
 * <p>
 * Note the {@link WatchService} is extremely OS dependant.  Mas OSX registers
 * very few events if they are done quickly and there is a significant delay
 * between the event and the event being triggered.
 */
public class FilePerKeyValueStore implements StringBytesStoreKeyValueStore, Closeable {
    private final Path dirPath;
    //Use BytesStore so that it can be shared safely between threads
    private final Map<File, FileRecord<BytesStore>> lastFileRecordMap = new ConcurrentHashMap<>();

    private final Thread fileFpmWatcher;
    private final VanillaSubscriptionKVSCollection<String, Bytes, BytesStore> subscriptions = new VanillaSubscriptionKVSCollection<>(this);
    private volatile boolean closed = false;
    private Asset asset;

    public FilePerKeyValueStore(RequestContext context, Asset asset) throws IORuntimeException {
        this(context.type(), context.basePath(), context.name());
        asset.registerView(StringBytesStoreKeyValueStore.class, this);
    }

    FilePerKeyValueStore(Class type, String basePath, String name) {
        assert type == String.class;

        String first = basePath;
        String dirName = first == null ? name : first + "/" + name;
        this.dirPath = Paths.get(dirName);
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
    public SubscriptionKVSCollection<String, BytesStore> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public long size() {
        return getFiles().count();
    }

    @Override
    public BytesStore getUsing(String key, Bytes value) {
        Path path = dirPath.resolve(key);
        return getFileContents(path, value);
    }

    @Override
    public void keysFor(int segment, SubscriptionConsumer<String> stringConsumer) {
        try {
            keysFor0(stringConsumer);
        } catch (InvalidSubscriberException ise) {
            // ignored
        }
    }

    void keysFor0(SubscriptionConsumer<String> stringConsumer) throws InvalidSubscriberException {
        getFiles().forEach(p -> {
            try {
                stringConsumer.accept(p.getFileName().toString());
            } catch (InvalidSubscriberException e) {
                throw Jvm.rethrow(e);
            }
        });
    }

    @Override
    public void entriesFor(int segment, SubscriptionConsumer<MapReplicationEvent<String, BytesStore>> kvConsumer) throws InvalidSubscriberException {
        try {
            entriesFor0(kvConsumer);
        } catch (InvalidSubscriberException ise) {
            // ignored
        }
    }

    void entriesFor0(SubscriptionConsumer<MapReplicationEvent<String, BytesStore>> kvConsumer) throws InvalidSubscriberException {
        getFiles().map(p -> EntryEvent.of(p.getFileName().toString(), getFileContents(p, null), 0, p.toFile().lastModified()))
                .forEach(e -> {
                    try {
                        kvConsumer.accept(e);
                    } catch (InvalidSubscriberException ise) {
                        throw Jvm.rethrow(ise);
                    }
                });
    }

    @Override
    public Iterator<Map.Entry<String, BytesStore>> entrySetIterator() {
        return getEntryStream().iterator();
    }

    public Stream<Map.Entry<String, BytesStore>> getEntryStream() {
        return getFiles()
                .map(p -> (Map.Entry<String, BytesStore>) new AbstractMap.SimpleEntry<>(p.getFileName().toString(), getFileContents(p, null)));
    }

    @Override
    public void put(String key, BytesStore value) {
        if (closed) throw new IllegalStateException("closed");
        Path path = dirPath.resolve(key);
        FileRecord fr = lastFileRecordMap.get(path.toFile());
        writeToFile(path, value);
        if (fr != null) fr.valid = false;
    }

    @Override
    public BytesStore getAndPut(String key, BytesStore value) {
        if (closed) throw new IllegalStateException("closed");
        Path path = dirPath.resolve(key);
        FileRecord fr = lastFileRecordMap.get(path.toFile());
        BytesStore existingValue = getFileContents(path, null);
        writeToFile(path, value);
        if (fr != null) fr.valid = false;
        return existingValue == null ? null : existingValue.bytes();
    }

    @Override
    public BytesStore getAndRemove(String key) {
        if (closed) throw new IllegalStateException("closed");
        BytesStore existing = get(key);
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
            return Files
                    .walk(dirPath)
                    .filter(p -> !Files.isDirectory(p))
                    .filter(this::isVisible);
        } catch (IOException e) {
            throw Jvm.rethrow(e);
        }
    }

    boolean isVisible(Path p) {
        return !p.getFileName().startsWith(".");
    }

    BytesStore getFileContents(Path path, Bytes using) {
        try {
            File file = path.toFile();
            FileRecord<BytesStore> lastFileRecord = lastFileRecordMap.get(file);
            if (lastFileRecord != null && lastFileRecord.valid
                    && file.lastModified() == lastFileRecord.timestamp) {
                return lastFileRecord.contents.bytes();
            }
            return getFileContentsFromDisk(path, using);
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    Bytes getFileContentsFromDisk(Path path, Bytes using) throws IOException {
        if (!Files.exists(path)) return null;
        File file = path.toFile();

        Buffers b = Buffers.BUFFERS.get();
        Bytes<ByteBuffer> readingBytes = b.valueBuffer;
        try (FileChannel fc = new FileInputStream(file).getChannel()) {
            readingBytes.ensureCapacity(fc.size());

            ByteBuffer dst = readingBytes.underlyingObject();
            dst.clear();

            fc.read(dst);

            readingBytes.position(0);
            readingBytes.limit(dst.position());
            dst.flip();
        }
        return readingBytes;
    }

    private void writeToFile(Path path, BytesStore value) {
        BytesStore<?, ByteBuffer> writingBytes;
        if (value.underlyingObject() instanceof ByteBuffer) {
            writingBytes = value;
        } else {
            Buffers b = Buffers.BUFFERS.get();
            Bytes<ByteBuffer> valueBuffer = b.valueBuffer;
            valueBuffer.clear();
            valueBuffer.write(value);
            valueBuffer.flip();
            writingBytes = valueBuffer;
        }

        File file = path.toFile();
        File tmpFile = new File(file.getParentFile(), "." + file.getName());
        try (FileChannel fc = new FileOutputStream(tmpFile).getChannel()) {
            ByteBuffer byteBuffer = writingBytes.underlyingObject();
            byteBuffer.position(0);
            byteBuffer.limit((int) writingBytes.limit());
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
    public Asset asset() {
        return asset;
    }

    @Override
    public KeyValueStore<String, Bytes, BytesStore> underlying() {
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
        private WatchKey processKey() throws InterruptedException, IOException, InvalidSubscriberException {
            WatchKey key = watcher.take();
            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();

                if (kind == StandardWatchEventKinds.OVERFLOW) {
                    // todo log a warning.
                    continue;
                }

                // get file fullName
                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path fileName = ev.context();
                String mapKey = fileName.toString();

                if (mapKey.startsWith(".")) {
                    //this avoids temporary files being added to the map
                    continue;
                }

                if (kind == StandardWatchEventKinds.ENTRY_CREATE || kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                    Path p = dirPath.resolve(fileName);
                    try {
                        Bytes mapVal = getFileContentsFromDisk(p, null);

                        FileRecord<BytesStore> prev = mapVal == null ? lastFileRecordMap.get(p.toFile())
                                : lastFileRecordMap.put(p.toFile(), new FileRecord<>(p.toFile().lastModified(), mapVal.copy()));
                        if (prev == null) {
                            subscriptions.notifyEvent(InsertedEvent.of(p.toFile().getName(), mapVal, 0, p.toFile().lastModified()));
                        } else {
                            subscriptions.notifyEvent(UpdatedEvent.of(p.toFile().getName(), prev.contents.bytes(), mapVal, 0, p.toFile().lastModified()));
                            prev.contents.release();
                        }
                    } catch (FileNotFoundException ignored) {
                    }

                } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                    Path p = dirPath.resolve(fileName);

                    FileRecord<BytesStore> prev = lastFileRecordMap.remove(p.toFile());
                    BytesStore lastVal = prev == null ? null : prev.contents;
                    subscriptions.notifyEvent(RemovedEvent.of(p.toFile().getName(), lastVal, 0, p.toFile().lastModified()));
                    if (prev != null)
                        prev.contents.release();
                }
            }
            return key;
        }
    }
}

