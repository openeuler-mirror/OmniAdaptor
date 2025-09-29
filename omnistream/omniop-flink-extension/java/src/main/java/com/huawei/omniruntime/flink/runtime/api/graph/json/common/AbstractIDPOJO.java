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

package com.huawei.omniruntime.flink.runtime.api.graph.json.common;

import static org.apache.flink.util.Preconditions.checkState;

import org.apache.flink.util.AbstractID;

import java.util.Objects;

/**
 * AbstractIDPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class AbstractIDPOJO {
    private long upperPart;
    private long lowerPart;

    public AbstractIDPOJO() {
        // Default constructor
    }

    public AbstractIDPOJO(AbstractID abstractID) {
        this.upperPart = abstractID.getUpperPart();
        this.lowerPart = abstractID.getLowerPart();
    }

    public AbstractIDPOJO(long upperPart, long lowerPart) {
        this.upperPart = upperPart;
        this.lowerPart = lowerPart;
    }

    public long getUpperPart() {
        return upperPart;
    }

    public void setUpperPart(long upperPart) {
        this.upperPart = upperPart;
    }

    public long getLowerPart() {
        return lowerPart;
    }

    public void setLowerPart(long lowerPart) {
        this.lowerPart = lowerPart;
    }

    @Override
    public String toString() {
        return "AbstractIDPOJO{"
                + "upperPart=" + upperPart
                + ", lowerPart=" + lowerPart
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        checkState(o instanceof AbstractIDPOJO, "o is not AbstractIDPOJO");
        AbstractIDPOJO that = (AbstractIDPOJO) o;
        return upperPart == that.upperPart && lowerPart == that.lowerPart;
    }

    @Override
    public int hashCode() {
        return Objects.hash(upperPart, lowerPart);
    }
}

