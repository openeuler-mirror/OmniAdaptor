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

package com.huawei.omniruntime.flink.runtime.shuffle;

import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.List;

// TBD
/**
 * OmniShuffleEnvironment
 *
 * @since 2025-04-27
 */
public interface OmniShuffleEnvironment {
    /**
     * getNativeShuffleServiceRef
     *
     * @return long
     */
    long getNativeShuffleServiceRef();

    /**
     * createShuffleIOOwnerContext
     *
     * @param taskNameWithSubtaskAndId taskNameWithSubtaskAndId
     * @param executionId executionId
     * @return OmniShuffleIOOwnerContext
     */
    OmniShuffleIOOwnerContext createShuffleIOOwnerContext(
            String taskNameWithSubtaskAndId,
            ExecutionAttemptID executionId);

    /**
     * createResultPartitionWriters
     *
     * @param ownerContext ownerContext
     * @param resultPartitionDeploymentDescriptors resultPartitionDeploymentDescriptors
     * @return List
     */
    List createResultPartitionWriters(
            OmniShuffleIOOwnerContext ownerContext,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors);
}
