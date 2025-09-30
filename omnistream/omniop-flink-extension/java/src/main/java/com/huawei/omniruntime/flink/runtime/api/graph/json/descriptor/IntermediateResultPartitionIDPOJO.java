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

import com.huawei.omniruntime.flink.runtime.api.graph.json.common.AbstractIDPOJO;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

/**
 * IntermediateResultPartitionIDPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class IntermediateResultPartitionIDPOJO {
    private AbstractIDPOJO intermediateDataSetID;
    private int partitionNum;

    public IntermediateResultPartitionIDPOJO() {
    }

    public IntermediateResultPartitionIDPOJO(IntermediateResultPartitionID intermediateResultPartitionID) {
        this.partitionNum = intermediateResultPartitionID.getPartitionNumber();
        this.intermediateDataSetID =
                new AbstractIDPOJO(intermediateResultPartitionID.getIntermediateDataSetID().getUpperPart(),
                intermediateResultPartitionID.getIntermediateDataSetID().getLowerPart());
    }

    public IntermediateResultPartitionIDPOJO(AbstractIDPOJO intermediateDataSetID, int partitionNum) {
        this.intermediateDataSetID = intermediateDataSetID;
        this.partitionNum = partitionNum;
    }

    public AbstractIDPOJO getIntermediateDataSetID() {
        return intermediateDataSetID;
    }

    public void setIntermediateDataSetID(AbstractIDPOJO intermediateDataSetID) {
        this.intermediateDataSetID = intermediateDataSetID;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    @Override
    public String toString() {
        return "IntermediateResultPartitionIDPOJO{"
                + "intermediateDataSetID=" + intermediateDataSetID
                + ", partitionNum=" + partitionNum
                + '}';
    }
}