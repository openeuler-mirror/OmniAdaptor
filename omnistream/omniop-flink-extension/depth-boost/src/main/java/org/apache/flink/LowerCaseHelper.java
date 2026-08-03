package org.apache.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Arrays;

public class LowerCaseHelper {
    private static final Unsafe UNSAFE;
    private static final long VALUE_OFFSET;

    private static final Logger LOG = LoggerFactory.getLogger(LowerCaseHelper.class);

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);

            VALUE_OFFSET = UNSAFE.objectFieldOffset(
                    String.class.getDeclaredField("value"));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String toLowerCaseFast(String input) throws InstantiationException {

        if (input == null || input.isEmpty()) {
            return input;
        }

        if (input.length() <= 100) {
            return input.toLowerCase();
        }

        char[] value = (char[]) UNSAFE.getObject(input, VALUE_OFFSET);
        byte coder = 1;

        char[] copy = Arrays.copyOf(value, value.length);
        nativeLower(copy, coder);

        String res = new String(copy);

        return res;
    }

    private static native void nativeLower(char[] value, byte coder);
}
