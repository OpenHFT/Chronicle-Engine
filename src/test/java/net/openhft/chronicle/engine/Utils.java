package net.openhft.chronicle.engine;

import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;

/**
 * Created by Rob Austin
 */
public class Utils {

    static String splitCamelCase(String s) {
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

        YamlLogging.showServerReads = false;
        YamlLogging.clientReads = false;
        YamlLogging.title = name;
    }

    public static void yamlLoggger(Runnable r) {
        try {
            YamlLogging.clientWrites = true;
            YamlLogging.clientReads = true;
            r.run();
        } finally {
            YamlLogging.clientWrites = false;
            YamlLogging.clientReads = false;
        }
    }
}
