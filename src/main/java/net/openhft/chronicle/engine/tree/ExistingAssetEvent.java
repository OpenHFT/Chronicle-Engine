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

import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

/**
 * Created by peter on 22/05/15.
 */
public class ExistingAssetEvent extends AbstractMarshallable implements TopologicalEvent {
    private String assetName;
    private String name;
    private Set<Class> classes;

    private ExistingAssetEvent(String assetName, String name, Set<Class> viewTypes) {
        this.assetName = assetName;
        this.name = name;
        this.classes = viewTypes;
    }

    @NotNull
    public static ExistingAssetEvent of(String assetName, String name, Set<Class> viewTypes) {
        return new ExistingAssetEvent(assetName, name, viewTypes);
    }


    @Override
    public boolean added() {
        return true;
    }

    @Override
    public String assetName() {
        return assetName;
    }

    public String name() {
        return name;
    }

    @Override
    public Set<Class> viewTypes() {
        return classes;
    }

    @NotNull
    @Override
    public String toString() {
        return "ExistingAssetEvent{" +
                "assetName='" + assetName + '\'' +
                ", name='" + name + '\'' +
                ", viewTypes=" + viewTypes() +
                '}';
    }
}