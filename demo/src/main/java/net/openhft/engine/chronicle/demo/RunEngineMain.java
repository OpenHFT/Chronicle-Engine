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

package net.openhft.engine.chronicle.demo;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.cfg.EngineClusterContext;
import net.openhft.engine.chronicle.demo.data.EndOfDay;
import net.openhft.engine.chronicle.demo.data.EndOfDayShort;

import java.io.IOException;

/**
 * Run EngineMain in test mode so slf4j will be imported.
 * Created by peter on 26/08/15.
 */
public class RunEngineMain {
    public static void main(String[] args) throws IOException {


        addClass(EndOfDay.class);
        addClass(EndOfDayShort.class);
        addClass(EngineClusterContext.class);
        // commented out as this class in not found
        //  addClass(NfsCfg.class);
        EngineMain.main(args);
    }

    public static void addClass(Class aClass) {
        ClassAliasPool.CLASS_ALIASES.addAlias(aClass);
    }


}
