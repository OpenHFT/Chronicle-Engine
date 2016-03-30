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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;

import java.util.function.BooleanSupplier;

/**
 * Created by Rob Austin
 */
public class Utils {

    static String splitCamelCase(@NotNull String s) {
        return s.replaceAll(
                String.format("%s|%s|%s",
                        "(?<=[A-Z])(?=[A-Z][a-z])",
                        "(?<=[^A-Z])(?=[A-Z])",
                        "(?<=[A-Za-z])(?=[^A-Za-z])"
                ),
                " "
        );
    }

    public static void methodName(@NotNull String methodName) {
        String methodName1 = (methodName.startsWith("test"))
                ? methodName.substring("test".length())
                : methodName;
        final String name = Utils.splitCamelCase(methodName1);

        YamlLogging.showServerReads(false);
        YamlLogging.showClientReads(false);
        YamlLogging.title = name;
    }

    public static void yamlLoggger(@NotNull Runnable r) {
        try {
            YamlLogging.showClientWrites(true);
            YamlLogging.showClientReads(true);
            YamlLogging.showServerWrites(true);
            YamlLogging.showServerReads(true);
            r.run();
        } finally {
            YamlLogging.showClientWrites(false);
            YamlLogging.showClientReads(false);
            YamlLogging.showServerWrites(false);
            YamlLogging.showServerReads(false);
        }
    }

    public static void waitFor(BooleanSupplier test) {
        for (int i = 1; i < 30; i++) {
            if (test.getAsBoolean())
                break;
            Jvm.pause(i * i);
        }
    }
}
