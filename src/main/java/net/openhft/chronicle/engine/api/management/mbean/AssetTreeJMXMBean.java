/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.api.management.mbean;

/**
 * Created by pct25 on 6/16/2015.
 */
public interface AssetTreeJMXMBean {

    String getEntries();

    long getSize();

    void setSize(long size);

    String getKeyType();

    String getValueType();

    int getTopicSubscriberCount();

    int getKeySubscriberCount();

    int getEntrySubscriberCount();

    String getPath();

    String getKeyStoreValue();

    void notifyMe(String key, String value);
}
