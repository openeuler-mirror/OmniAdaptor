/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.omniruntime.flink.runtime.io.network.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OmniResultPartitionManager
 *
 * @version 1.0.0
 * @since 2025/04/25
 */

public class OmniResultPartitionManager {
    private static final Logger LOG = LoggerFactory.getLogger(OmniResultPartitionManager.class);

    private long nativeResultPartitionManagerAddress;

    /**
     * registerResultPartition
     *
     * @param partition partition
     */
    public void registerResultPartition(OmniResultPartition partition) {
        long partitionAddress = partition.getNativeResultPartitionAddress();

        LOG.info("Registered {}.", partition);

        registerResultPartitionNative(nativeResultPartitionManagerAddress, partitionAddress);
        /**
         * synchronized (registeredPartitions) {
         * checkState(!isShutdown, "Result partition manager already shut down.");
         *
         * ResultPartition previous =
         * registeredPartitions.put(partition.getPartitionId(), partition);
         *
         * if (previous != null) {
         * throw new IllegalStateException("Result partition already registered.");
         * }
         *
         * LOG.debug("Registered {}.", partition);
         * } **/
    }

    private native void registerResultPartitionNative(
            long nativeResultPartitionManagerAddress,
            long nativePartitionAddress);
}
