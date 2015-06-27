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

package net.openhft.chronicle.engine.api.pubsub;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.api.tree.View;

/**
 * Created by peter on 22/05/15.
 */
public interface Subscription<E> extends View, Closeable {
    void registerSubscriber(RequestContext rc, Subscriber<E> subscriber);

    void unregisterSubscriber(Subscriber<E> subscriber);

    /**
     * @return total subscriber count.
     */
    int subscriberCount();
}
