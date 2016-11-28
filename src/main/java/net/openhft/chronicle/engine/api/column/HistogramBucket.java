package net.openhft.chronicle.engine.api.column;

/**
 * @author Rob Austin.
 */
public class HistogramBucket {
    int order;
    String lable;
    int count;

    public int order() {
        return order;
    }

    public String label() {
        return lable;
    }

    public int count() {
        return count;
    }
}
