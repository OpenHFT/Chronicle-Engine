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

package net.openhft.chronicle.engine.api.query;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Rob Austin.
 */
public class MarshableQueryTest {

    @Test
    public void testToPredicateFalse() throws Exception {

        VanillaIndexQuery marshableQuery = new VanillaIndexQuery();
        marshableQuery.select(TestBean.class, "value.x == 2");

        boolean test = marshableQuery.filter().test(new TestBean(5));
        Assert.assertEquals(false, test);

    }

    @Test
    public void testToPredicateTrue() throws Exception {

        VanillaIndexQuery marshableQuery = new VanillaIndexQuery();
        marshableQuery.select(TestBean.class, "value.x == 2");

        boolean test = marshableQuery.filter().test(new TestBean(2));
        Assert.assertEquals(true, test);
    }
}
