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
        @NotNull String methodName1 = (methodName.startsWith("test"))
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

    public static void waitFor(@NotNull BooleanSupplier test) {
        for (int i = 1; i < 30; i++) {
            if (test.getAsBoolean())
                break;
            Jvm.pause(i * i);
        }
    }
}
