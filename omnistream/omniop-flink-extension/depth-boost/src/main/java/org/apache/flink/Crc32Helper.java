package org.apache.flink;

/**
 * ARM SVE-accelerated CRC32 via native crc32d instruction.
 * Follows ReplaceHelper / LowerCaseHelper JNI pattern.
 *
 * crc32UpdateVoid: void method used by Agent MemberSubstitution
 * to replace java.util.zip.CRC32.update(byte[],int,int) calls.
 * Use crc32GetLast() to retrieve the computed CRC32 value.
 *
 * crc32Fast: direct static CRC32 computation (standalone use).
 */
public class Crc32Helper {

    private static final ThreadLocal<Long> CRC_LAST = ThreadLocal.withInitial(() -> 0L);

    /**
     * Void wrapper for Agent interception — replaces CRC32.update(byte[],int,int).
     * Computes JNI CRC32 and stores result in thread-local for later retrieval.
     */
    public static void crc32UpdateVoid(byte[] data, int offset, int len) {
        CRC_LAST.set(nativeCrc32(data, offset, len) & 0xFFFFFFFFL);
    }

    /** Retrieve the last CRC32 value computed by crc32UpdateVoid on this thread. */
    public static long crc32GetLast() {
        return CRC_LAST.get();
    }

    /** Direct static CRC32 computation. */
    public static long crc32Fast(byte[] data, int offset, int len) {
        return nativeCrc32(data, offset, len) & 0xFFFFFFFFL;
    }

    private static native int nativeCrc32(byte[] data, int offset, int len);
}
