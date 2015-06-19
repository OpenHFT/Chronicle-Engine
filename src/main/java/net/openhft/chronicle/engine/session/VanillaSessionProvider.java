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

package net.openhft.chronicle.engine.session;

import net.openhft.chronicle.engine.api.SessionDetails;
import net.openhft.chronicle.engine.api.session.SessionProvider;
import net.openhft.chronicle.engine.api.tree.View;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 01/06/15.
 */
public class VanillaSessionProvider implements SessionProvider, View {
    @NotNull
    private ThreadLocal<SessionDetails> sessionDetails = new ThreadLocal<>();

    public VanillaSessionProvider() {

    }

    @Override
    public SessionDetails get() {
        return this.sessionDetails.get();
    }

    @Override
    public void set(SessionDetails sessionDetails) {
        this.sessionDetails.set(sessionDetails);
    }

    @Override
    public void remove() {
        sessionDetails.remove();
    }
}
