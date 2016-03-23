/*
 *
 *  *     Copyright (C) ${YEAR}  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.openhft.chronicle.network.HeaderTcpHandler.toHeader;

/**
 * Created by Rob Austin
 */
public class UberHandler extends CspTcpHander<EngineWireNetworkContext> implements
        Demarshallable, WriteMarshallable {

    private static final Logger LOG = LoggerFactory.getLogger(UberHandler.class);

    private byte remoteIdentifier;
    private byte localIdentifier;
    private EventLoop eventLoop;
    private Asset rootAsset;

    @UsedViaReflection
    private UberHandler(WireIn wire) {
        remoteIdentifier = wire.read(() -> "hostId").int8();
        final WireType wireType = wire.read(() -> "wireType").object(WireType.class);
        wireType(wireType);
    }

    public UberHandler(byte localIdentifier,
                       byte remoteIdentifier,
                       WireType wireType) {
        assert remoteIdentifier != localIdentifier :
                "remoteIdentifier=" + remoteIdentifier + ", " +
                        "localIdentifier=" + localIdentifier;
        this.localIdentifier = localIdentifier;
        this.remoteIdentifier = remoteIdentifier;

        wireType(wireType);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "hostId").int8(localIdentifier);
        final WireType value = wireType();
        wire.write(() -> "wireType").object(value);
        wire.writeComment("remoteIdentifier=" + remoteIdentifier);
    }

    @Override
    public void nc(EngineWireNetworkContext nc) {
        super.nc(nc);

        nc.wireType(wireType());

        isAcceptor(nc.isAcceptor());
        rootAsset = nc.rootAsset();
        final HostIdentifier hostIdentifier = rootAsset.findOrCreateView(HostIdentifier.class);

        if (hostIdentifier != null)
            localIdentifier = hostIdentifier.hostId();


        assert remoteIdentifier != localIdentifier :
                "remoteIdentifier=" + remoteIdentifier + ", " +
                        "localIdentifier=" + localIdentifier;

        publisher(nc.wireOutPublisher());

        this.eventLoop = rootAsset.findOrCreateView(EventLoop.class);
        eventLoop.start();

        if (nc.isAcceptor())
            // reflect the header back to the client
            nc.wireOutPublisher().put("",
                    toHeader(new UberHandler(localIdentifier, remoteIdentifier, wireType
                            ()), localIdentifier, remoteIdentifier));
    }


    @Override
    protected void process(@NotNull WireIn inWire, @NotNull WireOut outWire) {

        if (YamlLogging.showServerReads)
            LOG.info("server read:\n" + Wires.fromSizePrefixedBlobs(inWire.bytes()));

        try (final DocumentContext dc = inWire.readingDocument()) {

            if (!dc.isPresent())
                return;

            if (dc.isMetaData()) {
                if (!readMeta(inWire))
                    return;

                handler().remoteIdentifier(remoteIdentifier);
                handler().onBootstrap(outWire);
                return;
            }

            if (dc.isData() && handler() != null)
                handler().processData(inWire, outWire);

        }

    }
}