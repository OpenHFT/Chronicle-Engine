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

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.util.StringUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 05/07/15.
 */
public class RequestContextInterner {
    @NotNull
    private final StringToRequestContext[] interner;
    private final int mask;

    public RequestContextInterner(int capacity) {
        int n = Maths.nextPower2(capacity, 128);
        interner = new StringToRequestContext[n];
        mask = n - 1;
    }

    public RequestContext intern(@NotNull CharSequence cs) {
        int h = Maths.hash32(cs) & mask;
        StringToRequestContext s = interner[h];
        if (s != null && StringUtils.isEqual(s.name, cs))
            return s.requestContext;
        String s2 = cs.toString();
        RequestContext rc = RequestContext.requestContext(cs);
        rc.seal();
        interner[h] = new StringToRequestContext(s2, rc);
        return rc;
    }

    static class StringToRequestContext {
        final String name;
        final RequestContext requestContext;

        StringToRequestContext(String name, RequestContext requestContext) {
            this.name = name;
            this.requestContext = requestContext;
        }
    }
}
