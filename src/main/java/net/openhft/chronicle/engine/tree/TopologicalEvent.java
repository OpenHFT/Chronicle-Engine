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

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.tree.ChangeEvent;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Set;

/**
 * Created by peter on 11/06/15.
 */
public interface TopologicalEvent extends ChangeEvent {
    String name();

    boolean added();

    /**
     * @return provides a list of the view avaible a the URI defined by {@code fullName}
     */
    default Set<Class> viewTypes() {
        return Collections.emptySet();
    }

    @NotNull
    default String fullName() {
        String parent = assetName();
        return parent == null ? "/"
                : (parent.isEmpty() || parent.equals("/")) ? "/" + name()
                : parent + "/" + name();
    }

    enum TopologicalFields implements WireKey {
        assetName, name
    }
}
