/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
