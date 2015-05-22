package net.openhft.chronicle.engine.utils;

import org.jetbrains.annotations.NotNull;

import static java.lang.Character.toLowerCase;

/**
 * Created by Rob Austin
 */
public enum StringUtils {
    ;

    public static boolean endsWith(@NotNull final CharSequence source,
                                   @NotNull final String endsWith) {
        for (int i = 1; i <= endsWith.length(); i++) {
            if (toLowerCase(source.charAt(source.length() - i)) !=
                    toLowerCase(endsWith.charAt(endsWith.length() - i))) {
                return false;
            }
        }

        return true;
    }

    public static boolean contains(@NotNull final CharSequence source,
                                   @NotNull final String token) {
        OUTER:
        for (int sourceI = 0; sourceI <= source.length() - token.length(); sourceI++) {
            int lastSourceI = sourceI;
            for (int tokenI = 0; tokenI < token.length(); tokenI++) {
                if (toLowerCase(source.charAt(sourceI)) != toLowerCase(token.charAt(tokenI))) {
                    sourceI = lastSourceI;
                    continue OUTER;
                }
                sourceI++;
            }
            return true;
        }
        return false;
    }
}
