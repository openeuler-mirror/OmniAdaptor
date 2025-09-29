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

package com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain;

/**
 * "kind": "logical",
 * "isNull": true,
 * "precision": 3,
 * "type": "TIMESTAMP",
 * "timestampKind": 0
 */

public class TypeDescriptionPOJO {
    private String kind;
    private boolean isNull;
    private int precision;
    private String type; // could be json string or simple type name like "Integer"
    private int timestampKind;
    private String fieldName = "";

    public TypeDescriptionPOJO() {
    }

    public TypeDescriptionPOJO(String kind, boolean isNull, int precision, String type, int timestampKind, String fieldName) {
        this.kind = kind;
        this.isNull = isNull;
        this.precision = precision;
        this.type = type;
        this.timestampKind = timestampKind;
        this.fieldName = fieldName;
    }

    public boolean isIsNull() {
        return isNull;
    }

    public void setIsNull(boolean aNull) {
        isNull = aNull;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int getTimestampKind() {
        return timestampKind;
    }

    public void setTimestampKind(int timestampKind) {
        this.timestampKind = timestampKind;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public String toString() {
        return "TypeDescriptionPOJO{" +
                "kind='" + kind + '\'' +
                ", isNull=" + isNull +
                ", precision=" + precision +
                ", type='" + type + '\'' +
                ", timestampKind=" + timestampKind +
                ", fieldName=" + fieldName +
                '}';
    }
}