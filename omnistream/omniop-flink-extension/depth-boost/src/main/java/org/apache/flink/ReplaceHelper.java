package org.apache.flink;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Arrays;

public class ReplaceHelper {

    private static final Unsafe UNSAFE;
    private static final long VALUE_OFFSET;
    private static final long MODE_LITERAL_TO_SINGLE = 1L;
    private static final long MODE_CLEAN_CONTROL_CHARS = 2L;
    private static final long NO_ACTION = 0L;
    private static final long ACTION_CLEAN_CONTROL_CHARS = MODE_CLEAN_CONTROL_CHARS << 56;
    private static final String CONTROL_CHARS_REGEX = "[\\x00\\r\\n\\t]";
    private static final FastCase[] FAST_CASES = new FastCase[] {
            cleanCase(CONTROL_CHARS_REGEX, "", ACTION_CLEAN_CONTROL_CHARS),


            literalToSingle("\t", " "),
            literalToSingle("\\t", " "),
            literalToSingle("\n", " "),
            literalToSingle("\\n", " "),
            literalToSingle("\r", " "),
            literalToSingle("\\r", " "),
            literalToSingle("\f", " "),
            literalToSingle("\\f", " "),

            literalToSingle(",", " "),
            literalToSingle(";", " "),
            literalToSingle(":", " "),
            literalToSingle("_", " "),
            literalToSingle("-", " "),
            literalToSingle("\\-", " "),
            literalToSingle("/", " "),
            literalToSingle("\\|", " "),
            literalToSingle("\\.", " "),
            literalToSingle("\\\\", "/"),
            literalToSingle("\\\\", " "),

            literalToSingle("\r\n", "\n"),
            literalToSingle("\\r\\n", "\n"),
            literalToSingle("\r\n", " "),
            literalToSingle("\\r\\n", " "),
            literalToSingle("\n\n", "\n"),
            literalToSingle("\\n\\n", "\n"),
            literalToSingle("\t\t", "\t"),
            literalToSingle("\\t\\t", "\t"),
            literalToSingle("\t\t", " "),
            literalToSingle("\\t\\t", " "),
            literalToSingle("  ", " "),
            literalToSingle("__", "_"),
            literalToSingle("--", "-"),
            literalToSingle("//", "/")
    };
    private static final FastCase[][][] FAST_CASE_BUCKETS = buildBuckets(FAST_CASES);

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);

            VALUE_OFFSET = UNSAFE.objectFieldOffset(
                    String.class.getDeclaredField("value"));
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static String replaceAllFast(String input, String regex, String replacement) throws InstantiationException {
        if (input == null || input.isEmpty()) {
            return input;
        }

        long actionCode = matchFastAction(regex, replacement);
        if (actionCode != NO_ACTION) {
            char[] value = (char[]) UNSAFE.getObject(input, VALUE_OFFSET);
            byte coder = 1;
            // Copy prevents native code from mutating the original String value.
            char[] copy = Arrays.copyOf(value, value.length);
            int resLen = nativeReplaceAll(copy, coder, actionCode);
            return new String(copy, 0, resLen);
        }

        String nativeResult = nativeReplaceAllGeneric(input, regex, replacement);
        if (nativeResult != null) {
            return nativeResult;
        }
        return input.replaceAll(regex, replacement);
    }

    private static long matchFastAction(String regex, String replacement) {
        if (regex == null || replacement == null) {
            return NO_ACTION;
        }
        int regexLength = regex.length();
        int replacementLength = replacement.length();
        if (regexLength >= FAST_CASE_BUCKETS.length
                || replacementLength >= FAST_CASE_BUCKETS[regexLength].length) {
            return NO_ACTION;
        }
        FastCase[] bucket = FAST_CASE_BUCKETS[regexLength][replacementLength];
        if (bucket == null) {
            return NO_ACTION;
        }
        for (FastCase fastCase : bucket) {
            if (fastCase.matches(regex, replacement)) {
                return fastCase.actionCode;
            }
        }
        return NO_ACTION;
    }

    private static FastCase[][][] buildBuckets(FastCase[] fastCases) {
        int maxRegexLength = 0;
        int maxReplacementLength = 0;
        for (FastCase fastCase : fastCases) {
            maxRegexLength = Math.max(maxRegexLength, fastCase.regex.length());
            maxReplacementLength = Math.max(maxReplacementLength, fastCase.replacement.length());
        }

        int[][] counts = new int[maxRegexLength + 1][maxReplacementLength + 1];
        for (FastCase fastCase : fastCases) {
            counts[fastCase.regex.length()][fastCase.replacement.length()]++;
        }

        FastCase[][][] buckets = new FastCase[maxRegexLength + 1][maxReplacementLength + 1][];
        for (int regexLength = 0; regexLength < counts.length; regexLength++) {
            for (int replacementLength = 0; replacementLength < counts[regexLength].length; replacementLength++) {
                int count = counts[regexLength][replacementLength];
                if (count > 0) {
                    buckets[regexLength][replacementLength] = new FastCase[count];
                }
            }
        }

        int[][] offsets = new int[maxRegexLength + 1][maxReplacementLength + 1];
        for (FastCase fastCase : fastCases) {
            int regexLength = fastCase.regex.length();
            int replacementLength = fastCase.replacement.length();
            buckets[regexLength][replacementLength][offsets[regexLength][replacementLength]++] = fastCase;
        }
        return buckets;
    }

    private static FastCase cleanCase(String regex, String replacement, long actionCode) {
        return new FastCase(regex, replacement, actionCode);
    }

    private static FastCase literalToSingle(String regex, String replacement) {
        if (replacement == null || replacement.length() != 1) {
            throw new IllegalArgumentException("literal-to-single replacement must contain one char");
        }
        char[] literal = parseListedLiteralRegex(regex);
        char old1 = literal[0];
        char old2 = literal.length == 2 ? literal[1] : 0;
        long actionCode = encodeLiteralToSingleAction(old1, old2, literal.length, replacement.charAt(0));
        return new FastCase(regex, replacement, actionCode);
    }

    private static char[] parseListedLiteralRegex(String regex) {
        if (regex == null || regex.isEmpty()) {
            throw new IllegalArgumentException("literal regex must contain 1-2 chars");
        }

        char[] chars = new char[2];
        int outLen = 0;
        for (int i = 0; i < regex.length(); i++) {
            char ch = regex.charAt(i);
            if (ch == '\\') {
                if (++i >= regex.length()) {
                    throw new IllegalArgumentException("dangling escape in literal regex: " + regex);
                }
                int escaped = escapedLiteralChar(regex.charAt(i));
                if (escaped < 0) {
                    throw new IllegalArgumentException("unsupported escape in literal regex: " + regex);
                }
                ch = (char) escaped;
            } else if (isRegexMetaChar(ch)) {
                throw new IllegalArgumentException("regex meta char must be escaped in fast case: " + regex);
            }
            if (outLen >= chars.length) {
                throw new IllegalArgumentException("literal regex must contain 1-2 chars: " + regex);
            }
            chars[outLen++] = ch;
        }
        if (outLen == 0) {
            throw new IllegalArgumentException("literal regex must contain 1-2 chars");
        }
        return outLen == 1 ? new char[] {chars[0]} : chars;
    }

    private static boolean isRegexMetaChar(char ch) {
        return "\\.^$|?*+()[]{}".indexOf(ch) >= 0;
    }

    private static int escapedLiteralChar(char escaped) {
        switch (escaped) {
            case 't':
                return '\t';
            case 'n':
                return '\n';
            case 'r':
                return '\r';
            case 'f':
                return '\f';
            case '\\':
            case '.':
            case '^':
            case '$':
            case '|':
            case '?':
            case '*':
            case '+':
            case '(':
            case ')':
            case '[':
            case ']':
            case '{':
            case '}':
            case '-':
                return escaped;
            default:
                return -1;
        }
    }

    private static long encodeLiteralToSingleAction(char old1, char old2, int oldLen, char newChar) {
        return (MODE_LITERAL_TO_SINGLE << 56)
                | ((long) oldLen << 48)
                | ((long) old1 << 32)
                | ((long) old2 << 16)
                | newChar;
    }

    private static final class FastCase {
        private final String regex;
        private final String replacement;
        private final long actionCode;

        private FastCase(String regex, String replacement, long actionCode) {
            this.regex = regex;
            this.replacement = replacement;
            this.actionCode = actionCode;
        }

        private boolean matches(String regex, String replacement) {
            return this.regex.equals(regex) && this.replacement.equals(replacement);
        }
    }

    private static native int nativeReplaceAll(char[] value, byte coder, long actionCode);

    private static native String nativeReplaceAllGeneric(String input, String regex, String replacement);
}
