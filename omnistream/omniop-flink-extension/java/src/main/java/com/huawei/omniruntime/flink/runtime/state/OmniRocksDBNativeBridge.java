package com.huawei.omniruntime.flink.runtime.state;

/** Uses the Omni library loaded by TNELLibrary before TaskExecutor is constructed. */
final class OmniRocksDBNativeBridge {
    private OmniRocksDBNativeBridge() {}

    static native long createSharedResources(long totalMemoryBytes, double writeBufferRatio,
            double highPriorityRatio, boolean partitionedFilters);

    static native void releaseSharedResources(long handle);
}
