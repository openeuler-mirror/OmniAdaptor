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

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * OmniShuffleEnvironmentShadow
 *
 * @since 2025-04-27
 */
public class OmniShuffleEnvironmentShadow implements OmniShuffleEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(OmniShuffleEnvironmentShadow.class);

    private long nativeShuffleEnvironmentAddress;


    public OmniShuffleEnvironmentShadow(long nativeShuffleServiceRef) {
        this.nativeShuffleEnvironmentAddress = nativeShuffleServiceRef;
    }


    public long getNativeShuffleEnvironmentAddress() {
        return nativeShuffleEnvironmentAddress;
    }

    @Override
    public long getNativeShuffleServiceRef() {
        return 0;
    }


    @Override
    public OmniShuffleIOOwnerContext createShuffleIOOwnerContext(
            String ownerName, ExecutionAttemptID executionAttemptID) {
        return new OmniShuffleIOOwnerContext(
                checkNotNull(ownerName),
                checkNotNull(executionAttemptID));
    }


    @Override
    public List createResultPartitionWriters(OmniShuffleIOOwnerContext ownerContext,
                                             List<ResultPartitionDeploymentDescriptor>
                                                     resultPartitionDeploymentDescriptors) {
        // call native implemeantion
        return null;
    }
}
