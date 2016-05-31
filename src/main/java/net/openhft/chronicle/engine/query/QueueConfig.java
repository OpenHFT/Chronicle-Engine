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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.tree.InitializableBiConsumer;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;


/**
 * @author Rob Austin.
 */
public class QueueConfig {

    private final Function<String, Integer> sourceB;
    private final boolean acknowledgment;
    private final InitializableBiConsumer<Wire, Bytes> messageAdaptor;

    /**
     * @param queueSource
     * @param acknowledgment each replication event sends back an enableAcknowledgment, which is
     *                       then stored in the chronicle queue.
     */
    public QueueConfig(Function<String, Integer> queueSource, boolean acknowledgment,
                       @Nullable InitializableBiConsumer<Wire, Bytes> messageAdaptor) {
        this.sourceB = queueSource;
        this.messageAdaptor = messageAdaptor;
        this.acknowledgment = acknowledgment;
    }

    public Integer sourceHostId(@NotNull String uri) {
        return sourceB.apply(uri);
    }

    public boolean acknowledgment() {
        return acknowledgment;
    }

    public InitializableBiConsumer<Wire, Bytes> bytesFunction() {
        return messageAdaptor;
    }

}
