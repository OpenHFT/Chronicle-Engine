package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.column.ChronicleQueueRow;
import net.openhft.chronicle.engine.api.column.ClosableIterator;
import net.openhft.chronicle.engine.api.column.Column;
import net.openhft.chronicle.engine.api.column.QueueColumnView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.ObjectSubscription;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.FieldInfo;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static java.util.Spliterators.spliteratorUnknownSize;
import static net.openhft.chronicle.core.util.ObjectUtils.convertTo;
import static net.openhft.chronicle.queue.TailerDirection.BACKWARD;
import static net.openhft.chronicle.wire.Wires.fieldInfos;

/**
 * @author Rob Austin.
 */
public class QueueWrappingColumnView<K, V> implements QueueColumnView {

    private final Asset asset;
    @NotNull
    private final QueueView<String, V> queueView;
    @Nullable
    private ArrayList<String> columnNames = null;
    private final Class<?> messageClass;

    public QueueWrappingColumnView(
            RequestContext requestContext,
            Asset asset,
            @NotNull QueueView<String, V> queueView) {
        this.asset = asset;
        this.queueView = queueView;

        @Nullable final QueueView.Excerpt<String, V> excerpt = queueView.getExcerpt(0);
        if (excerpt != null)
            messageClass = excerpt.message().getClass();
        else
            messageClass = Object.class;

    }

    @Override
    public void registerChangeListener(@NotNull Runnable r) {
        queueView.registerSubscriber("", o -> r.run());
    }

    private ClosableIterator<ChronicleQueueRow> iteratorWithCountFromEnd(
            @NotNull final List<MarshableFilter> filters,
            long countFromEnd) {
        final long index = toIndexFromEnd(countFromEnd);
        return toIterator(filters, index);
    }

    @NotNull
    @Override
    public ClosableIterator<ChronicleQueueRow> iterator(@NotNull final SortedFilter filters) {
        long countFromEnd = filters.countFromEnd;
        if (countFromEnd > 0)
            return iteratorWithCountFromEnd(filters.marshableFilters, countFromEnd);
        else
            return iterator(filters.marshableFilters, filters.fromIndex);
    }

    @NotNull
    Map<List<MarshableFilter>, NavigableMap<Long, ChronicleQueueRow>> indexCache = new ConcurrentHashMap<>();

    @NotNull
    private ClosableIterator<ChronicleQueueRow> iterator(@NotNull final List<MarshableFilter> filters, long fromSequenceNumber) {
        long count = 0;
        final NavigableMap<Long, ChronicleQueueRow> map = indexCache.computeIfAbsent(filters, k -> new ConcurrentSkipListMap<>());
        final Map.Entry<Long, ChronicleQueueRow> longChronicleQueueRowEntry = map.floorEntry(fromSequenceNumber);

        final long index0;

        if (longChronicleQueueRowEntry != null) {
            ChronicleQueueRow value = longChronicleQueueRowEntry.getValue();
            count = value.seqNumber();
            index0 = longChronicleQueueRowEntry.getValue().index();
        } else
            index0 = 0;

        @NotNull final ClosableIterator<ChronicleQueueRow> result = toIterator(filters, index0);

        @Nullable ChronicleQueueRow r = null;
        while (count < fromSequenceNumber && result.hasNext()) {
            r = result.next();
            count++;
            r.seqNumber(count);
            if (r.seqNumber() % 1024 == 0)
                map.put(count, r);

        }

        if (longChronicleQueueRowEntry == null && r != null)
            map.put(r.seqNumber(), r);

        return result;
    }

    @NotNull
    private ClosableIterator<ChronicleQueueRow> toIterator(@NotNull List<MarshableFilter> filters, final long index) {
        @Nullable final Iterator<QueueView.Excerpt<String, V>> i = new Iterator<QueueView.Excerpt<String, V>>() {

            @Nullable
            QueueView.Excerpt<String, V> next = queueView.getExcerpt(index);

            @Override
            public boolean hasNext() {
                if (next == null)
                    next = queueView.getExcerpt("");
                return next != null;
            }

            @Nullable
            @Override
            public QueueView.Excerpt<String, V> next() {
                if (this.next == null)
                    throw new NoSuchElementException();
                try {
                    return this.next;
                } finally {
                    this.next = null;
                }
            }
        };

        final Spliterator<QueueView.Excerpt<String, V>> spliterator = spliteratorUnknownSize(i,
                Spliterator.DISTINCT | Spliterator.SORTED | Spliterator.ORDERED);
        final Iterator<QueueView.Excerpt<String, V>> core = StreamSupport.stream(spliterator,
                false)
                .filter(filter(filters))
                .iterator();

        return new ClosableIterator<ChronicleQueueRow>() {

            @Override
            public void close() {
                // do nothing
            }

            @Override
            public boolean hasNext() {
                return core.hasNext();
            }

            @NotNull
            @Override
            public ChronicleQueueRow next() {
                final QueueView.Excerpt<String, V> e = core.next();
                @NotNull final ChronicleQueueRow row = new ChronicleQueueRow(columns());
                @NotNull final Object value = e.message();

                row.set("index", Long.toHexString(e.index()));
                row.index(e.index());

                for (@NotNull final FieldInfo info : fieldInfos(value.getClass())) {

                    if (!columnNames().contains(info.name()))
                        continue;
                    try {

                        final Object newValue = info.get(value);
//                            System.out.println("\t"+newValue);
                        row.set(info.name(), newValue);

                    } catch (Exception e1) {
                        Jvm.warn().on(VanillaMapView.class, e1);
                    }
                }

                return row;
            }
        };
    }

    private long toIndexFromEnd(long countFromEnd) {
        long fromSequenceNumber = -1;

        if (countFromEnd > 0) {
            @Nullable final SingleChronicleQueue chronicleQueue = (SingleChronicleQueue) queueView.underlying();

            @NotNull final ExcerptTailer excerptTailer = chronicleQueue.createTailer().direction(BACKWARD).toEnd();
            fromSequenceNumber = excerptTailer.index();
            if (fromSequenceNumber == 0)
                return 0;

            for (int i = 0; i < countFromEnd; i++) {

                try (DocumentContext documentContext = excerptTailer.readingDocument()) {
                    if (!documentContext.isPresent())
                        break;
                    fromSequenceNumber = documentContext.index();
                }
            }

        }
        return fromSequenceNumber;
    }

    @Override
    public boolean containsRowWithKey(@NotNull List keys) {
        if (keys.size() == 1 && keys.get(0) instanceof String) {
            final long l = Long.parseLong(keys.get(0).toString(), 16);
            return queueView.getExcerpt(l) != null;
        }

        throw new IllegalStateException("unsupported format, keys=" + keys);
    }

    @Nullable
    @Override
    public ObjectSubscription objectSubscription() {
        return queueView.asset().getView(ObjectSubscription.class);
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @NotNull
    @Override
    public List<Column> columns() {
        @NotNull List<Column> result = new ArrayList<>();

        result.add(new Column("index", true, true, "", String.class, false));

        for (@NotNull final FieldInfo fi : fieldInfos(messageClass)) {
            result.add(new Column(fi.name(), false, false, "", fi.type(), false));
        }

        return result;
    }

    @Nullable
    private List<String> columnNames() {

        if (columnNames != null)
            return columnNames;

        @NotNull LinkedHashSet<String> result = new LinkedHashSet<>();
        result.add("index");

        // if (Marshallable.class.isAssignableFrom(messageClass)) {
        for (@NotNull final FieldInfo fi : fieldInfos(messageClass)) {
            result.add(fi.name());
        }
        //  }

        columnNames = new ArrayList<>(result);
        return columnNames;
    }


    @Override
    public boolean canDeleteRows() {
        return false;
    }


    @Override
    public int changedRow(@NotNull Map<String, Object> row, @NotNull Map<String, Object> oldRow) {
        // chronicle queue is read only
        return 0;
    }

    @Nullable
    public Predicate<QueueView.Excerpt<String, V>> filter(@Nullable List<MarshableFilter> filters) {

        @Nullable final Predicate predicate = predicate(filters);

        return excerpt -> {

            if (filters == null || filters.isEmpty())
                return true;

            try {

                for (@NotNull MarshableFilter f : filters) {

                    Object item;
                    final Class messageClass = excerpt.message().getClass();

                    if (Marshallable.class.isAssignableFrom(messageClass)) {
                        try {

                            // final Field field = messageClass.getDeclaredField(f.columnName);
                            V message = excerpt.message();
                            FieldInfo info = Wires.fieldInfo(message.getClass(), f.columnName);
                            final Object o = info.get(message);
                            //   field.setAccessible(true);
                            // final Object o = field.get(excerpt.message());

                            if (o == null)
                                return false;
                            if (o instanceof Number) {
                                if (predicate.test(o))
                                    continue;
                                return false;
                            }
                            item = o;

                        } catch (Exception e) {
                            return false;
                        }

                    } else {
                        throw new UnsupportedOperationException();
                    }

                    if (item instanceof CharSequence) {
                        if (!item.toString().toLowerCase().contains(f.filter.toLowerCase()))
                            return false;
                    } else if (item instanceof Number) {
                        if (!predicate.test(item))
                            return false;
                    } else {
                        if (!item.equals(convertTo(item.getClass(), f.filter.trim())))
                            return false;
                    }

                }

                return true;

            } catch (NumberFormatException e) {
                return false;
            }
        };
    }


    /**
     * @param filters if {@code sortedFilter} == null or empty all the total number of rows is
     *                returned
     * @return the number of rows the matches this query
     */
    @Override
    public int rowCount(@NotNull SortedFilter filters) {

        long fromSequenceNumber = filters.fromIndex;
        long countFromEnd = filters.countFromEnd;

        assert countFromEnd != 0 && fromSequenceNumber == 0;

        if (countFromEnd > 0) {
            int count0 = 0;
            @NotNull ClosableIterator<ChronicleQueueRow> iterator = iterator(filters);

            while (iterator.hasNext()) {
                iterator.next();
                count0++;
            }
            return count0;
        }


        final NavigableMap<Long, ChronicleQueueRow> map = indexCache.computeIfAbsent(filters.marshableFilters, k
                -> new ConcurrentSkipListMap<>());

        Iterator<ChronicleQueueRow> iterator;

        long count = 0;
        if (map != null) {
            final Map.Entry<Long, ChronicleQueueRow> last = map.lastEntry();
            if (last == null) {
                iterator = iterator(filters);
            } else {
                final ChronicleQueueRow value = last.getValue();
                count = value.seqNumber();
                filters.fromIndex = last.getValue().index();
                iterator = iterator(filters);
            }
        } else
            iterator = iterator(filters);

        boolean hasMoreData = false;
        @Nullable ChronicleQueueRow row = null;

        while (iterator.hasNext()) {
            row = iterator.next();
            count++;
            row.seqNumber(count);
            hasMoreData = true;
            if (row.seqNumber() % 1024 == 0)
                map.put(count, row);
        }

        if (hasMoreData)
            map.put(count, row);

        return (int) count;
    }

}