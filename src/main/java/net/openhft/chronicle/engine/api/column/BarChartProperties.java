package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.wire.AbstractMarshallable;

/**
 * @author Rob Austin.
 */
public class BarChartProperties extends AbstractMarshallable {
    public String title = "";
    public Integer pointWidth;
    public String yAxisTitle = "";
}
