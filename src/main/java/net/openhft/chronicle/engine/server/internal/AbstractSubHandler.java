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

import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.api.session.SubHandler;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public abstract class AbstractSubHandler<T extends NetworkContext> implements SubHandler<T> {
    private T nc;
    private long cid;
    private String csp;
    private byte remoteIdentifier;

    @Override
    public void cid(long cid) {
        this.cid = cid;
    }

    @Override
    public long cid() {
        return cid;
    }

    @Override
    public void csp(@NotNull String csp) {
        this.csp = csp;
    }

    @Override
    public String csp() {
        return this.csp;
    }

    @Override
    public abstract void processData(@NotNull WireIn inWire, @NotNull WireOut outWire);

    @Override
    public T nc() {
        return nc;
    }

    @Override
    public void nc(T nc) {
        this.nc = nc;
    }

    public byte remoteIdentifier() {
        return remoteIdentifier;
    }

    @Override
    public void remoteIdentifier(byte remoteIdentifier) {
        this.remoteIdentifier = remoteIdentifier;
    }
}
