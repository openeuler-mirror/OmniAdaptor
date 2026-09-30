package com.huawei.omniruntime.flink.runtime.state;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.util.function.LongFunctionWithException;

/** Slot sharing and reservation are delegated to Flink; RocksDB allocation stays in Omni. */
public final class OmniRocksDBSharedResourcesFactory {
    private static final String MANAGED_ID = "omni-rocks-managed-memory-v1";
    private static final String FIXED_ID = "omni-rocks-fixed-slot-memory-v1";

    private OmniRocksDBSharedResourcesFactory() {}

    public static double getStateBackendFractionOfSlot(
            StreamConfig taskConfig, Configuration taskManagerConfig, ClassLoader classLoader) {
        // Native operators can introduce keyed state behind a stateless Java chain head.
        // Every backend shares the slot's complete STATE_BACKEND allocation, whose size
        // still follows Flink's consumer weights rather than the head operator's usage.
        StreamConfig slotConfig = new StreamConfig(new Configuration(taskConfig.getConfiguration()));
        slotConfig.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.STATE_BACKEND, 1.0);
        return slotConfig.getManagedMemoryFractionOperatorUseCaseOfSlot(
                ManagedMemoryUseCase.STATE_BACKEND, taskManagerConfig, classLoader);
    }

    public static OpaqueMemoryResource<OmniRocksDBSharedResources> acquire(
            MemoryManager manager, double fraction, OmniRocksDBMemoryConfiguration configuration)
            throws Exception {
        return acquire(manager, fraction, configuration, size -> {
            long handle = OmniRocksDBNativeBridge.createSharedResources(size,
                    configuration.writeBufferRatio, configuration.highPriorityRatio,
                    configuration.partitionedFilters);
            return new OmniRocksDBSharedResources(handle, size, configuration,
                    OmniRocksDBNativeBridge::releaseSharedResources);
        });
    }

    static OpaqueMemoryResource<OmniRocksDBSharedResources> acquire(
            MemoryManager manager, double fraction, OmniRocksDBMemoryConfiguration configuration,
            LongFunctionWithException<OmniRocksDBSharedResources, Exception> allocator) throws Exception {
        final OpaqueMemoryResource<OmniRocksDBSharedResources> lease;
        if (configuration.fixedBytes > 0) {
            lease = manager.getExternalSharedMemoryResource(FIXED_ID, allocator, configuration.fixedBytes);
        } else if (configuration.managed) {
            if (!Double.isFinite(fraction) || fraction <= 0 || fraction > 1) {
                throw new IllegalArgumentException("Omni RocksDB requires a positive slot memory fraction");
            }
            lease = manager.getSharedMemoryResourceForManagedMemory(MANAGED_ID, allocator, fraction);
        } else {
            throw new IllegalArgumentException("Omni RocksDB requires managed or fixed-per-slot memory");
        }
        long expectedBytes = configuration.fixedBytes > 0
                ? configuration.fixedBytes : manager.computeMemorySize(fraction);
        if (!lease.getResourceHandle().configuration.matches(configuration)
                || lease.getSize() != expectedBytes) {
            lease.close();
            throw new IllegalArgumentException("Conflicting Omni RocksDB memory policies in one slot");
        }
        return lease;
    }
}
