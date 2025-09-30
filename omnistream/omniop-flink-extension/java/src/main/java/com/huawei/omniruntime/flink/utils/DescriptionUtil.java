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

package com.huawei.omniruntime.flink.utils;

import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * manage the system's configuration information
 *
 * @since 2025/04/22
 * @version 1.0.0
 */
public class DescriptionUtil {
    /**
     * getFieldType
     *
     * @param fieldType fieldType
     * @return String
     */
    public static String getFieldType(LogicalType fieldType) {
        LogicalTypeRoot typeRoot = fieldType.getTypeRoot();
        String typeName = typeRoot.toString();
        if (typeRoot == LogicalTypeRoot.VARCHAR) {
            if (fieldType instanceof VarCharType) {
                VarCharType varcharType = (VarCharType) fieldType;
                typeName += "(" + varcharType.getLength() + ")";
            }
        }
        if (typeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE
                || typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            // Cast to TimestampType to access precision
            if (fieldType instanceof TimestampType) {
                int precision = ((TimestampType) fieldType).getPrecision();
                typeName += "(" + precision + ")";
            }
        }
        if (typeRoot == LogicalTypeRoot.DECIMAL) {
            if (fieldType instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) fieldType;
                Integer precision = decimalType.getPrecision();
                Integer scale = decimalType.getScale();
                if (precision > 19) {
                    typeName += "128";
                } else {
                    typeName += "64";
                }
                typeName += "(" + precision + "," + scale + ")";
            }
        }
        return typeName;
    }
}
