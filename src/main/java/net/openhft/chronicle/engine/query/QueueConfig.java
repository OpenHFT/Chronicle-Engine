/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
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

package net.openhft.chronicle.engine.query;

import net.openhft.chronicle.engine.tree.MessageAdaptor;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public class QueueConfig {

    private final Function<String, Integer> sourceB;
    private final boolean acknowledgment;
    private final MessageAdaptor messageAdaptor;
    private final WireType wireType;

    /**
     * @param queueSource
     * @param acknowledgment each replication event sends back an enableAcknowledgment, which is
     *                       then stored in the chronicle queue.
     */
    public QueueConfig(Function<String, Integer> queueSource, boolean acknowledgment,
                       @Nullable MessageAdaptor messageAdaptor, WireType wireType) {
        this.sourceB = queueSource;
        this.messageAdaptor = messageAdaptor;
        this.acknowledgment = acknowledgment;
        this.wireType = wireType;
    }

    public Integer sourceHostId(@NotNull String uri) {
        return sourceB.apply(uri);
    }

    public boolean acknowledgment() {
        return acknowledgment;
    }

    public MessageAdaptor bytesFunction() {
        return messageAdaptor;
    }

    @NotNull
    public WireType wireType() {
        return wireType;
    }
}
