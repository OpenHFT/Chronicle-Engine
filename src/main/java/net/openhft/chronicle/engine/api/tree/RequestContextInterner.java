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
