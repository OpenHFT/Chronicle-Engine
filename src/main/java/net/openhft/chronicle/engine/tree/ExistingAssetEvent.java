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

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Set;

/**
 * Created by peter on 22/05/15.
 */
public class ExistingAssetEvent extends AbstractMarshallable implements TopologicalEvent {
    private String assetName;
    private String name;
    private Set<Class> classes;

    private ExistingAssetEvent(String assetName, Asset asset) {
        this.assetName = assetName;
        this.name = asset.name();
        this.classes = asset.viewTypes();
    }

    private ExistingAssetEvent(String assetName, String name) {
        this.assetName = assetName;
        this.classes = Collections.emptySet();
        this.name = name;
    }

    @NotNull
    public static ExistingAssetEvent of(String assetName, Asset asset) {
        return new ExistingAssetEvent(assetName, asset);
    }

    /**
     * @param assetName the name of the asset path
     * @param name      the name of the asset
     * @return an instance of ExistingAssetEvent
     * @deprecated only used for testing, use net.openhft.chronicle.engine.tree" +
     * ".ExistingAssetEvent#of(java.lang.String, net.openhft.chronicle.engine.api.tree" + ".Asset)
     * instead
     */
    @Deprecated()
    @NotNull
    public static ExistingAssetEvent of(String assetName, String name) {
        return new ExistingAssetEvent(assetName, name);
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

    @Override
    public String toString() {
        return "ExistingAssetEvent{" +
                "assetName='" + assetName + '\'' +
                ", name='" + name + '\'' +
                ", viewTypes=" + viewTypes() +
                '}';
    }
}