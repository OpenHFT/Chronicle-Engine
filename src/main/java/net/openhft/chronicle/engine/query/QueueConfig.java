/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    @NotNull
    private final Function<String, Integer> sourceB;
    private final boolean acknowledgment;
    @Nullable
    private final MessageAdaptor messageAdaptor;
    @NotNull
    private final WireType wireType;

    /**
     * @param queueSource
     * @param acknowledgment each replication event sends back an enableAcknowledgment, which is
     *                       then stored in the chronicle queue.
     */
    public QueueConfig(@NotNull Function<String, Integer> queueSource,
                       boolean acknowledgment,
                       @Nullable MessageAdaptor messageAdaptor,
                       @NotNull WireType wireType) {
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

    @Nullable
    public MessageAdaptor bytesFunction() {
        return messageAdaptor;
    }

    @NotNull
    public WireType wireType() {
        return wireType;
    }
}
