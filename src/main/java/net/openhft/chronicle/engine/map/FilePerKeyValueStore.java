/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine.map;

import com.sun.nio.file.SensitivityWatchEventModifier;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.StringBytesStoreKeyValueStore;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.threads.Threads;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.WatchEvent.Kind;
import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static net.openhft.chronicle.core.Jvm.pause;
import static net.openhft.chronicle.engine.api.EngineReplication.ReplicationEntry;

/**
 * A {@link Map} implementation that stores each entry as a file in a directory. The
 * <code>key</code> is the file fullName and the <code>value</code> is the contents of the file. <p>
 * The class is effectively an abstraction over a directory in the file system. Therefore when the
 * underlying files are changed an event will be fired to those registered for notifications. <p>
 * Updates will be fired every time the file is saved but will be suppressed if the value has not
 * changed.  To avoid temporary files (e.g. if edited in vi) being included in the map, any file
 * starting with a '.' will be ignored. <p> Note the {@link WatchService} is extremely OS dependant.
 * Mas OSX registers very few events if they are done quickly and there is a significant delay
 * between the event and the event being triggered.
 */
public class FilePerKeyValueStore implements StringBytesStoreKeyValueStore, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(FilePerKeyValueStore.class);

    private final Path dirPath;
    //Use BytesStore so that it can be shared safely between threads
    private final Map<File, FileRecord<BytesStore>> lastFileRecordMap = new ConcurrentHashMap<>();

    @NotNull
    private final Thread fileFpmWatcher;
    @NotNull
    private final RawKVSSubscription<String, BytesStore> subscriptions;
    @NotNull
    private final Asset asset;

    private final WatchService watcher;
    private volatile boolean closed = false;

    public FilePerKeyValueStore(@NotNull RequestContext context, @NotNull Asset asset) throws IORuntimeException, AssetNotFoundException {
        this(context, asset, context.type(), context.basePath(), context.name());
        asset.registerView(StringBytesStoreKeyValueStore.class, this);
    }

    private FilePerKeyValueStore(RequestContext context, @NotNull Asset asset, Class type, String basePath, String name) throws AssetNotFoundException {
        this.asset = asset;
        assert type == String.class;

        String first = basePath;
        String dirName = first == null ? name : first + "/" + name;
        this.dirPath = Paths.get(dirName);

        try {
            Files.createDirectories(dirPath);
            watcher = FileSystems.getDefault().newWatchService();
            dirPath.register(watcher, new Kind[]{
                            StandardWatchEventKinds.ENTRY_CREATE,
                            StandardWatchEventKinds.ENTRY_DELETE,
                            StandardWatchEventKinds.ENTRY_MODIFY},
                    SensitivityWatchEventModifier.HIGH
            );
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }

        fileFpmWatcher = new Thread(new FPMWatcher(watcher), Threads.threadGroupPrefix() + " watcher for " + dirName);
        fileFpmWatcher.setDaemon(true);
        fileFpmWatcher.start();
        subscriptions = asset.acquireView(RawKVSSubscription.class, context);
        subscriptions.setKvStore(this);
    }

    @NotNull
    @Override
    public RawKVSSubscription<String, BytesStore> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public long longSize() {
        return getFiles().count();
    }

    @Nullable
    @Override
    public BytesStore getUsing(String key, Object value) {
        Path path = dirPath.resolve(key);
        return getFileContents(path, (Bytes) value);
    }

    @Override
    public void keysFor(int segment, @NotNull SubscriptionConsumer<String> stringConsumer) {
        keysFor0(stringConsumer);
    }

    private void keysFor0(@NotNull SubscriptionConsumer<String> stringConsumer) {
        getFiles().forEach(p -> {
            try {
                stringConsumer.accept(p.getFileName().toString());
            } catch (InvalidSubscriberException e) {
                throw Jvm.rethrow(e);
            }
        });
    }

    @Override
    public void entriesFor(int segment, @NotNull SubscriptionConsumer<MapEvent<String, BytesStore>> kvConsumer) throws InvalidSubscriberException {
        entriesFor0(kvConsumer);
    }

    private void entriesFor0(@NotNull SubscriptionConsumer<MapEvent<String, BytesStore>> kvConsumer) {
        getFiles().forEach(p -> {
            BytesStore fileContents = null;
            try {
                // in case the file has been deleted in the meantime.
                fileContents = getFileContents(p, null);
                if (fileContents != null) {
                    InsertedEvent e = InsertedEvent.of(asset.fullName(), p.getFileName().toString(), fileContents, false);
                    kvConsumer.accept(e);
                }
            } catch (InvalidSubscriberException ise) {
                throw Jvm.rethrow(ise);
            } finally {
                if (fileContents != null)
                    fileContents.release();
            }
        });
    }

    @Override
    public Iterator<String> keySetIterator() {
        return getFiles().map(p -> p.getFileName().toString()).iterator();
    }

    @Override
    public Iterator<Map.Entry<String, BytesStore>> entrySetIterator() {
        return getEntryStream().iterator();
    }

    private Stream<Map.Entry<String, BytesStore>> getEntryStream() {
        return getFiles()
                .map(p -> {
                    BytesStore fileContents = null;
                    try {
                        fileContents = getFileContents(p, null);
                        return (Map.Entry<String, BytesStore>) new SimpleEntry<>(p.getFileName().toString(), fileContents);
                    } finally {
                        if (fileContents != null)
                            fileContents.release();
                    }
                });
    }

    @Override
    public boolean put(String key, @NotNull BytesStore value) {
        if (closed) throw new IllegalStateException("closed");
        Path path = dirPath.resolve(key);
        FileRecord fr = lastFileRecordMap.get(path.toFile());
        writeToFile(path, value);
        if (fr != null) fr.valid = false;
        return fr != null;
    }

    // TODO mark return value as reserved.
    @Nullable
    @Override
    public BytesStore getAndPut(String key, @NotNull BytesStore value) {
        if (closed) throw new IllegalStateException("closed");
        Path path = dirPath.resolve(key);
        FileRecord fr = lastFileRecordMap.get(path.toFile());
        BytesStore existingValue = getFileContents(path, null);
        writeToFile(path, value);
        if (fr != null) fr.valid = false;
        return existingValue == null ? null : existingValue;
    }

    // TODO mark return value as reserved.
    @Nullable
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
    public boolean remove(String key) {
        if (closed) throw new IllegalStateException("closed");
        Path path = dirPath.resolve(key);
        if (path.toFile().isFile())
            deleteFile(path);
        // todo check this is removed in watcher
        FileRecord fr = lastFileRecordMap.get(path.toFile());
        return fr != null;
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
                // ignored at first.
            }
        });
        if (count.intValue() > 0) {
            pause(100);
            getFiles().forEach(this::deleteFile);
        }
    }

    @Override
    public boolean containsValue(final BytesStore value) {
        throw new UnsupportedOperationException("todo");
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

    private boolean isVisible(@NotNull Path p) {
        return !p.getFileName().startsWith(".");
    }

    @Nullable
    private BytesStore getFileContents(@NotNull Path path, Bytes using) {
        File file = path.toFile();
        FileRecord<BytesStore> lastFileRecord = lastFileRecordMap.get(file);
        if (lastFileRecord != null && lastFileRecord.valid
                && file.lastModified() == lastFileRecord.timestamp) {
            BytesStore contents = lastFileRecord.contents();
            if (contents != null)
                return contents;
        }
        return getFileContentsFromDisk(path, using);
    }

    @Nullable
    private Bytes getFileContentsFromDisk(@NotNull Path path, Bytes using) {
        for (int i = 1; i <= 5; i++) {
            try {
                return getFileContentsFromDisk0(path, using);
            } catch (IOException e) {
                pause(i * i * 2);
            }
        }
        return null;
    }

    private Bytes getFileContentsFromDisk0(@NotNull Path path, Bytes using) throws IOException {
        if (!Files.exists(path)) return null;
        File file = path.toFile();

        Buffers b = Buffers.BUFFERS.get();
        Bytes<ByteBuffer> readingBytes = b.valueBuffer;
        try (FileChannel fc = new FileInputStream(file).getChannel()) {
            readingBytes.ensureCapacity(fc.size());

            ByteBuffer dst = readingBytes.underlyingObject();
            dst.clear();

            fc.read(dst);

            readingBytes.readPosition(0);
            readingBytes.readLimit(dst.position());
            dst.flip();
        }
        readingBytes.reserve();
        return readingBytes;
    }

    private void writeToFile(@NotNull Path path, @NotNull BytesStore value) {
        BytesStore<?, ByteBuffer> writingBytes;
        if (value.underlyingObject() instanceof ByteBuffer) {
            writingBytes = value;
        } else {
            Buffers b = Buffers.BUFFERS.get();
            Bytes<ByteBuffer> valueBuffer = b.valueBuffer;
            valueBuffer.clear();
            valueBuffer.write(value);
            writingBytes = valueBuffer;
        }

        File file = path.toFile();
        File tmpFile = new File(file.getParentFile(), "." + file.getName() + "." + System.nanoTime());
        try (FileChannel fc = new FileOutputStream(tmpFile).getChannel()) {
            ByteBuffer byteBuffer = writingBytes.underlyingObject();
            byteBuffer.position(0);
            byteBuffer.limit((int) writingBytes.readLimit());
            fc.write(byteBuffer);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        for (int i = 1; i < 5; i++) {
            try {
                Files.move(tmpFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                break;

            } catch (FileSystemException fse) {
                if (LOG.isDebugEnabled())
                    LOG.debug("Unable to rename file " + fse);
                try {
                    Thread.sleep(i * i * 2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        System.out.println(file + " size: " + file.length());
    }

    private void deleteFile(@NotNull Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void close() {
        closed = true;
        fileFpmWatcher.interrupt();
        Closeable.closeQuietly(watcher);
    }

    @NotNull
    @Override
    public Asset asset() {
        return asset;
    }

    @Nullable
    @Override
    public KeyValueStore<String, BytesStore> underlying() {
        return null;
    }

    @Override
    public void accept(final ReplicationEntry replicationEntry) {
        throw new UnsupportedOperationException("todo");
    }

    private class FPMWatcher implements Runnable {
        private final WatchService watcher;

        public FPMWatcher(WatchService watcher) {
            this.watcher = watcher;
        }

        @Override
        public void run() {
            try {
                while (!closed) {
                    WatchKey key = null;
                    try {
                        key = processKey();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    } finally {
                        if (key != null) key.reset();
                    }
                }
            } catch (Throwable e) {
                if (!closed)
                    LOG.error("", e);
            }
        }

        @NotNull
        private WatchKey processKey() throws InterruptedException {
            WatchKey key = watcher.take();
            for (WatchEvent<?> event : key.pollEvents()) {
                Kind<?> kind = event.kind();

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
//                System.out.println("file: "+mapKey+" kind: "+kind);

                if (kind == StandardWatchEventKinds.ENTRY_CREATE || kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                    Path p = dirPath.resolve(fileName);
                    BytesStore mapVal = getFileContentsFromDisk(p, null);

                    FileRecord<BytesStore> prev = lastFileRecordMap.get(p.toFile());
//                    if (mapVal == null) {
//                            System.out.println("Unable to read "+mapKey+", exists: "+p.toFile().exists());
//                    }
                    BytesStore prevContents = prev == null ? null : prev.contents();
                    try {
                        if (mapVal != null && mapVal.contentEquals(prevContents)) {
//                            System.out.println("... key: "+mapKey+" equal, last.keys: "+new TreeSet<>(lastFileRecordMap.keySet()));
                            continue;
                        }

                        if (mapVal == null) {
                            // todo this shouldn't happen.
                            if (prev != null)
                                mapVal = prevContents;
                        } else {
//                            System.out.println("adding "+mapKey);
                            lastFileRecordMap.put(p.toFile(), new FileRecord<>(p.toFile().lastModified(), mapVal.copy()));
                        }
                        if (prev == null) {
                            subscriptions.notifyEvent(InsertedEvent.of(asset.fullName(), p.toFile().getName(), mapVal, false));
                        } else {
                            subscriptions.notifyEvent(UpdatedEvent.of(asset.fullName(), p.toFile
                                    ().getName(), prevContents, mapVal, false, prevContents ==
                                    null ? true :  !prevContents.equals(mapVal)));
                        }
                    } finally {
                        if (prevContents != null)
                            prevContents.release();
                    }

                } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                    Path p = dirPath.resolve(fileName);

                    FileRecord<BytesStore> prev = lastFileRecordMap.remove(p.toFile());
                    BytesStore lastVal = prev == null ? null : prev.contents();
                    try {
                        subscriptions.notifyEvent(RemovedEvent.of(asset.fullName(), p.toFile().getName(), lastVal, false));
                    } finally {
                        if (lastVal != null)
                            lastVal.release();
                    }
                }
            }
            return key;
        }
    }
}

