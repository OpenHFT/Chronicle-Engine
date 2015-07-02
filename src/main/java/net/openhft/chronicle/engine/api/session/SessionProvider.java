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

package net.openhft.chronicle.engine.api.session;

import net.openhft.chronicle.network.api.session.SessionDetails;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A holder for the Session speicifc details i.e. for a remote client.
 */
public interface SessionProvider {
    /**
     * @return the current session details
     */
    @Nullable
    SessionDetails get();

    /**
     * Replace the session details
     *
     * @param sessionDetails to set to
     */
    void set(@NotNull SessionDetails sessionDetails);

    /**
     * There is no longer any valid session detaisl and get() will return null.
     */
    void remove();
}
