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

package net.openhft.chronicle.engine.api.tree;

import org.jetbrains.annotations.NotNull;

/**
 * This factory creates a wrapping based on the context and the asset it is associated with.  A wrapping view expects to wrap an existing view.
 */
@FunctionalInterface
public interface WrappingViewFactory<I, U> {
    /**
     * Create a view for this asset based on the requestContext
     *
     * @param requestContext additional information to help build the view.
     * @param asset          to associate this view with
     * @param underlying     the implementation this view is expected to wrap.
     * @return the view
     * @throws AssetNotFoundException if the leaf node depends on something which could not be constructed.
     */
    @NotNull
    I create(RequestContext requestContext, Asset asset, U underlying) throws AssetNotFoundException;
}
