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
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static java.util.Spliterators.spliteratorUnknownSize;
import static net.openhft.chronicle.core.util.ObjectUtils.convertTo;

/**
 * @author Rob Austin.
 */
public class QueueWrappingColumnView<K, V> implements QueueColumnView {

    private final QueueView<String, V> queueView;
    @Nullable
    private ArrayList<String> columnNames = null;
    private final Class<?> messageClass;

    public QueueWrappingColumnView(
            RequestContext requestContext,
            Asset asset,
            QueueView<String, V> queueView) {
        this.queueView = queueView;

        final QueueView.Excerpt<String, V> excerpt = queueView.getExcerpt(0);
        if (excerpt != null)
            messageClass = excerpt.message().getClass();
        else
            messageClass = Object.class;

    }

    @Override
    public void registerChangeListener(@NotNull Runnable r) {
        queueView.registerSubscriber("", o -> r.run());
    }

    @NotNull
    @Override
    public ClosableIterator<ChronicleQueueRow> iterator(@NotNull final SortedFilter filters) {
        return iterator(filters.marshableFilters, filters.fromIndex);
    }

    Map<List<MarshableFilter>, NavigableMap<Long, ChronicleQueueRow>> indexCache = new ConcurrentHashMap<>();

    @NotNull
    private ClosableIterator<ChronicleQueueRow> iterator(@NotNull final List<MarshableFilter> filters, long fromSequenceNumber) {
        long count = 0;
        final NavigableMap<Long, ChronicleQueueRow> map = indexCache.computeIfAbsent(filters, k -> new ConcurrentSkipListMap<>());
        final Map.Entry<Long, ChronicleQueueRow> longChronicleQueueRowEntry = map.floorEntry(fromSequenceNumber);

        if (longChronicleQueueRowEntry != null)
            count = longChronicleQueueRowEntry.getValue().seqNumber();

        final Iterator<QueueView.Excerpt<String, V>> i = new Iterator<QueueView.Excerpt<String, V>>() {

            QueueView.Excerpt<String, V> next = (longChronicleQueueRowEntry != null)
                    ? queueView.getExcerpt(longChronicleQueueRowEntry.getValue().index())
                    : queueView.getExcerpt(0);

            @Override
            public boolean hasNext() {
                if (next == null)
                    next = queueView.getExcerpt("");
                return next != null;
            }

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

        final Spliterator<QueueView.Excerpt<String, V>> spliterator = spliteratorUnknownSize(i, Spliterator.DISTINCT | Spliterator.SORTED | Spliterator.ORDERED);
        final Iterator<QueueView.Excerpt<String, V>> core = StreamSupport.stream(spliterator,
                false)
                .filter(filter(filters))
                .iterator();

        @NotNull final ClosableIterator<ChronicleQueueRow> result = new ClosableIterator<ChronicleQueueRow>() {

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

                @NotNull final AbstractMarshallable value = (AbstractMarshallable) e.message();

                row.set("index", Long.toHexString(e.index()));
                row.index(e.index());

                for (@NotNull final Field declaredFields : value.getClass().getDeclaredFields()) {
                    if (!columnNames().contains(declaredFields.getName()))
                        continue;
                    try {
                        declaredFields.setAccessible(true);
                        row.set(declaredFields.getName(), declaredFields.get(value));
                    } catch (Exception e1) {
                        Jvm.warn().on(VanillaMapView.class, e1);
                    }
                }

                return row;
            }
        };

        ChronicleQueueRow r = null;
        while (count < fromSequenceNumber && result.hasNext()) {
            r = result.next();
            r.seqNumber(count);
            if (r.seqNumber() % 1024 == 0)
                map.put(count, r);
            count++;
        }

        if (longChronicleQueueRowEntry == null && r != null)
            map.put(r.seqNumber(), r);

        return result;
    }

    @Override
    public boolean containsRowWithKey(@NotNull Object[] keys) {
        if (keys.length == 1 && keys[0] instanceof String) {
            final long l = Long.parseLong(keys[0].toString(), 16);
            return queueView.getExcerpt(l) != null;
        }

        throw new IllegalStateException("unsupported format, keys=" + Arrays.toString(keys));
    }

    @Nullable
    @Override
    public ObjectSubscription objectSubscription() {
        return queueView.asset().getView(ObjectSubscription.class);
    }

    @NotNull
    @Override
    public List<Column> columns() {
        @NotNull List<Column> result = new ArrayList<>();

        result.add(new Column("index", true, true, "", String.class, false));

        for (@NotNull final Field declaredFields : messageClass.getDeclaredFields()) {
            result.add(new Column(declaredFields.getName(), false, false, "",
                    declaredFields.getType(), false));
        }

        return result;
    }

    @Nullable
    private ArrayList<String> columnNames() {

        if (columnNames != null)
            return columnNames;

        @NotNull LinkedHashSet<String> result = new LinkedHashSet<>();
        result.add("index");

        for (@NotNull final Field declaredFields : messageClass.getDeclaredFields()) {
            result.add(declaredFields.getName());
        }

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
        return excerpt -> {

            if (filters == null || filters.isEmpty())
                return true;

            try {

                for (@NotNull MarshableFilter f : filters) {

                    Object item;
                    final Class messageClass = excerpt.message().getClass();

                    if (AbstractMarshallable.class.isAssignableFrom(messageClass)) {
                        try {

                            final Field field = messageClass.getDeclaredField(f.columnName);
                            field.setAccessible(true);
                            final Object o = field.get(excerpt.message());

                            if (o == null)
                                return false;
                            if (o instanceof Number) {
                                if (toRange((Number) o, f.filter.trim()))
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
                        if (!toRange((Number) item, f.filter.trim()))
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

    private boolean toRange(@NotNull Number o, @NotNull String trimmed) {
        if (trimmed.startsWith(">") || trimmed.startsWith("<")) {

            @NotNull final String number = trimmed.substring(1, trimmed.length()).trim();

            final Number filterNumber;
            try {
                filterNumber = convertTo(o.getClass(), number);
            } catch (ClassCastException e) {
                return false;
            }

            boolean result;
            if (trimmed.startsWith(">"))
                result = o.doubleValue() > filterNumber.doubleValue();
            else if (trimmed.startsWith("<"))
                result = o.doubleValue() < filterNumber.doubleValue();
            else
                throw new UnsupportedOperationException();
            return result;

        } else {
            final Object filterNumber = convertTo(o.getClass(), trimmed);
            return o.equals(filterNumber);
        }
    }

    /**
     * @param filters if {@code sortedFilter} == null or empty all the total number of rows is
     *                returned
     * @return the number of rows the matches this query
     */
    @Override
    public int rowCount(@NotNull List<MarshableFilter> filters) {
        long count = 0;
        final NavigableMap<Long, ChronicleQueueRow> map = indexCache.computeIfAbsent(filters, k -> new ConcurrentSkipListMap<>());

        Iterator<ChronicleQueueRow> iterator;
        long lastIndex = 0;

        if (map != null) {
            final Map.Entry<Long, ChronicleQueueRow> last = map.lastEntry();
            if (last == null) {
                iterator = iterator(filters, 0);
            } else {
                final ChronicleQueueRow value = last.getValue();
                count = value.seqNumber();
                iterator = iterator(filters, count);
            }
        } else
            iterator = iterator(filters, 0);

        boolean hasMoreData = false;
        ChronicleQueueRow row = null;
        while (iterator.hasNext()) {
            row = iterator.next();
            row.seqNumber(count);
            count++;
            hasMoreData = true;

            if (row.seqNumber() % 1024 == 0)
                map.put(count, row);
        }

        if (hasMoreData) {

            map.put(count, row);
        }

        return (int) count;
    }

}