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

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;

/**
 * Created by peter on 25/05/15.
 */
public class Buffers {
    static final ThreadLocal<Buffers> BUFFERS = ThreadLocal.withInitial(Buffers::new);
    final Bytes<ByteBuffer> keyBuffer = Bytes.elasticByteBuffer();
    final Bytes<ByteBuffer> valueBuffer = Bytes.elasticByteBuffer();

    private Buffers() {
    }
}
