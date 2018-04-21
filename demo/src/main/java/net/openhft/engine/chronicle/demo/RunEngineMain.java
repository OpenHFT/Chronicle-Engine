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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.EngineInstance;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.cfg.EngineClusterContext;
import net.openhft.chronicle.network.cluster.handlers.UberHandler;
import net.openhft.chronicle.wire.YamlLogging;
import net.openhft.engine.chronicle.demo.data.EndOfDay;
import net.openhft.engine.chronicle.demo.data.EndOfDayShort;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;

/**
 * Run EngineMain in test mode so slf4j will be imported.
 * Created by Peter Lawrey on 26/08/15.
 */
public class RunEngineMain {
    static final int HOST_ID = Integer.getInteger("engine.hostId", 1);

    static {
        RequestContext.loadDefaultAliases();
        ClassAliasPool.CLASS_ALIASES.addAlias(UberHandler.Factory.class, "UberHandlerFactory");
        ClassAliasPool.CLASS_ALIASES.addAlias(EndOfDay.class, EndOfDayShort.class, EngineClusterContext.class);
    }

    public static void main(String[] args) throws IOException {
        YamlLogging.setAll(true);
        @NotNull String name = args.length > 0 ? args[0] : resolveConfigurationFile();
        EngineInstance.engineMain(HOST_ID, name, "cluster");
    }

    @NotNull
    private static String resolveConfigurationFile() {
        final File expectedDemoConfig = new File(OS.USER_DIR, "demo/src/main/resources/engine.yaml");
        if (expectedDemoConfig.exists()) {
            return expectedDemoConfig.getAbsolutePath();
        }
        return "engine.yaml";
    }
}
