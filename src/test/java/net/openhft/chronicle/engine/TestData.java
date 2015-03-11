/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine;

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireOut;

/**
 * Created by peter.lawrey on 31/01/15.
 */
class TestData implements Marshallable {
    int key1;
    long key2;
    double key3;

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(Field.key1).int32(key1)
                .write(Field.key2).int64(key2)
                .write(Field.key3).float64(key3);
    }

    @Override
    public void readMarshallable(WireIn wire) {
        wire.read(Field.key1).int32(i -> key1 = i)
                .read(Field.key2).int64(i -> key2 = i)
                .read(Field.key3).float64(i -> key3 = i);
    }

    enum Field implements WireKey {
        key1, key2, key3;
    }
}
