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
    Function<String, Integer> source;
    boolean acknowledgment;
    @Nullable
    MessageAdaptor messageAdaptor;
    @NotNull
    WireType wireType;

    /**
     * @param masterIDFunction a give a assert-URI returns the master ID
     * @param acknowledgment   each replication event sends back an enableAcknowledgment, which is
     *                         then stored in the chronicle queue.
     */
    public QueueConfig(@NotNull Function<String, Integer> masterIDFunction,
                       boolean acknowledgment,
                       @Nullable MessageAdaptor messageAdaptor,
                       @NotNull WireType wireType) {
        this.source = masterIDFunction;
        this.messageAdaptor = messageAdaptor;
        this.acknowledgment = acknowledgment;
        this.wireType = wireType;
    }

    public Integer sourceHostId(@NotNull String uri) {
        return source.apply(uri);
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
