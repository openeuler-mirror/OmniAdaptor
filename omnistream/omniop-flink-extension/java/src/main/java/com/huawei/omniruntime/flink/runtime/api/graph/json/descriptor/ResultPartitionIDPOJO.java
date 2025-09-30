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

package com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

/**
 * ResultPartitionIDPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class ResultPartitionIDPOJO {
    private IntermediateResultPartitionIDPOJO partitionId;
    private ExecutionAttemptIDPOJO producerId;

    public ResultPartitionIDPOJO() {

    }

    public ResultPartitionIDPOJO(ResultPartitionID resultPartitionID) {
        this.partitionId = new IntermediateResultPartitionIDPOJO(resultPartitionID.getPartitionId());
        this.producerId = new ExecutionAttemptIDPOJO(resultPartitionID.getProducerId());
    }

    public ResultPartitionIDPOJO(IntermediateResultPartitionIDPOJO partitionId, ExecutionAttemptIDPOJO producerId) {
        this.partitionId = partitionId;
        this.producerId = producerId;
    }

    public IntermediateResultPartitionIDPOJO getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(IntermediateResultPartitionIDPOJO partitionId) {
        this.partitionId = partitionId;
    }

    public ExecutionAttemptIDPOJO getProducerId() {
        return producerId;
    }

    public void setProducerId(ExecutionAttemptIDPOJO producerId) {
        this.producerId = producerId;
    }

    @Override
    public String toString() {
        return "ResultPartitionIDPOJO{"
                + "partitionId=" + partitionId
                + ", producerId=" + producerId
                + '}';
    }
}