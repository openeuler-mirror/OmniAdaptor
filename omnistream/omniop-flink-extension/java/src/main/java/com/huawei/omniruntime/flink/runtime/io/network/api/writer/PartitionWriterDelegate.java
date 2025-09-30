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

package com.huawei.omniruntime.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;

/**
 * The partition writer delegate provides the availability function for task processor, and it might
 * represent a single
 * {@link ResultPartitionWriter}
 * or multiple
 * {@link ResultPartitionWriter}
 * instances in specific
 * implementations.
 *
 * @version 1.0.0
 * @since 2025/04/24
 */
public interface PartitionWriterDelegate {
    /**
     * Returns the internal actual partittion writer instance based on the output index.
     *
     * @param outputIndex the index respective to the record writer instance.
     * @return {@link ResultPartitionWriter }
     */
    ResultPartitionWriter getPartitionWriter(int outputIndex);
}
