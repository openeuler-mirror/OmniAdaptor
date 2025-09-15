/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.spark.serialize;


import com.google.protobuf.InvalidProtocolBufferException;
import com.huawei.boostkit.spark.jni.NativeLoader;
import nova.hetu.omniruntime.type.*;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.*;
import nova.hetu.omniruntime.vector.serialize.OmniRowDeserializer;

import org.apache.spark.sql.execution.vectorized.OmniColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleDataSerializer {
    private static final Logger LOG = LoggerFactory.getLogger(NativeLoader.class);

    public static ColumnarBatch deserialize(boolean isRowShuffle, byte[] bytes) {
        if (!isRowShuffle) {
            return deserializeByColumn(bytes);
        } else {
            return deserializeByRow(bytes);
        }
    }

    public static ColumnarBatch deserializeByColumn(byte[] bytes) {
        ColumnVector[] vecs = null;
        try {
            VecData.VecBatch vecBatch = VecData.VecBatch.parseFrom(bytes);
            int vecCount = vecBatch.getVecCnt();
            int rowCount = vecBatch.getRowCnt();
            vecs = new ColumnVector[vecCount];
            for (int i = 0; i < vecCount; i++) {
                vecs[i] = buildVec(vecBatch.getVecs(i), rowCount);
            }
            return new ColumnarBatch(vecs, rowCount);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("deserialize failed. errmsg:" + e.getMessage());
        } catch (OmniRuntimeException e) {
            if (vecs != null) {
                for (int i = 0; i < vecs.length; i++) {
                    ColumnVector vec = vecs[i];
                    if (vec != null) {
                        vec.close();
                    }
                }
            }
            throw new RuntimeException("deserialize failed. errmsg:" + e.getMessage());
        }
    }

    public static ColumnarBatch deserializeByRow(byte[] bytes) {
        try {
            VecData.ProtoRowBatch rowBatch = VecData.ProtoRowBatch.parseFrom(bytes);
            int vecCount = rowBatch.getVecCnt();
            int rowCount = rowBatch.getRowCnt();
            OmniColumnVector[] columnarVecs = new OmniColumnVector[vecCount];
            long[] omniVecs = new long[vecCount];
            int[] omniTypes = new int[vecCount];
            createEmptyVec(rowBatch, omniTypes, omniVecs, columnarVecs, vecCount, rowCount);
            OmniRowDeserializer deserializer = new OmniRowDeserializer(omniTypes, omniVecs);

            for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                VecData.ProtoRow protoRow = rowBatch.getRows(rowIdx);
                byte[] array = protoRow.getData().toByteArray();
                deserializer.parse(array, rowIdx);
            }

            // update initial varchar vector because it's capacity might have been expanded.
            for (int i = 0; i < vecCount; i++) {
                if (omniTypes[i] == VarcharDataType.VARCHAR.getId().toValue()) {
                    Vec varcharVec = VecFactory.create(omniVecs[i], VecEncoding.OMNI_VEC_ENCODING_FLAT, VarcharDataType.VARCHAR);
                    columnarVecs[i].setVec(varcharVec);
                }
            }

            deserializer.close();
            return new ColumnarBatch(columnarVecs, rowCount);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("deserialize failed. errmsg:" + e.getMessage());
        }
    }

    private static ColumnVector buildVec(VecData.Vec protoVec, int vecSize) {
        VecData.VecType protoTypeId = protoVec.getVecType();
        Vec vec;
        DataType type;
        switch (protoTypeId.getTypeId()) {
            case VEC_TYPE_INT:
                type = DataTypes.IntegerType;
                vec = new IntVec(vecSize);
                break;
            case VEC_TYPE_DATE32:
                type = DataTypes.DateType;
                vec = new IntVec(vecSize);
                break;
            case VEC_TYPE_LONG:
                type = DataTypes.LongType;
                vec = new LongVec(vecSize);
                break;
            case VEC_TYPE_TIMESTAMP:
                type = DataTypes.TimestampType;
                vec = new LongVec(vecSize);
                break;
            case VEC_TYPE_DATE64:
                type = DataTypes.DateType;
                vec = new LongVec(vecSize);
                break;
            case VEC_TYPE_DECIMAL64:
                type = DataTypes.createDecimalType(protoTypeId.getPrecision(), protoTypeId.getScale());
                vec = new LongVec(vecSize);
                break;
            case VEC_TYPE_SHORT:
                type = DataTypes.ShortType;
                vec = new ShortVec(vecSize);
                break;
            case VEC_TYPE_BOOLEAN:
                type = DataTypes.BooleanType;
                vec = new BooleanVec(vecSize);
                break;
            case VEC_TYPE_DOUBLE:
                type = DataTypes.DoubleType;
                vec = new DoubleVec(vecSize);
                break;
            case VEC_TYPE_VARCHAR:
            case VEC_TYPE_CHAR:
                type = DataTypes.StringType;
                vec = new VarcharVec(protoVec.getValues().size(), vecSize);
                if (vec instanceof VarcharVec) {
                    ((VarcharVec) vec).setOffsetsBuf(protoVec.getOffset().toByteArray());
                }
                break;
            case VEC_TYPE_DECIMAL128:
                type = DataTypes.createDecimalType(protoTypeId.getPrecision(), protoTypeId.getScale());
                vec = new Decimal128Vec(vecSize);
                break;
            case VEC_TYPE_TIME32:
            case VEC_TYPE_TIME64:
            case VEC_TYPE_INTERVAL_DAY_TIME:
            case VEC_TYPE_INTERVAL_MONTHS:
            default:
                throw new IllegalStateException("Unexpected value: " + protoTypeId.getTypeId());
        }
        vec.setValuesBuf(protoVec.getValues().toByteArray());
        if(protoVec.getNulls().size() != 0) {
            vec.setNullsBuf(protoVec.getNulls().toByteArray());
        }
        OmniColumnVector vecTmp = new OmniColumnVector(vecSize, type, false);
        vecTmp.setVec(vec);
        return vecTmp;
    }

    public static void createEmptyVec(VecData.ProtoRowBatch rowBatch, int[] omniTypes, long[] omniVecs, OmniColumnVector[] columnarVectors, int vecCount, int rowCount) {
        for (int i = 0; i < vecCount; i++) {
            VecData.VecType protoTypeId = rowBatch.getVecTypes(i);
            DataType sparkType;
            Vec omniVec;
            switch (protoTypeId.getTypeId()) {
                case VEC_TYPE_INT:
                    sparkType = DataTypes.IntegerType;
                    omniTypes[i] = IntDataType.INTEGER.getId().toValue();
                    omniVec = new IntVec(rowCount);
                    break;
                case VEC_TYPE_DATE32:
                    sparkType = DataTypes.DateType;
                    omniTypes[i] = Date32DataType.DATE32.getId().toValue();
                    omniVec = new IntVec(rowCount);
                    break;
                case VEC_TYPE_LONG:
                    sparkType = DataTypes.LongType;
                    omniTypes[i] = LongDataType.LONG.getId().toValue();
                    omniVec = new LongVec(rowCount);
                    break;
                case VEC_TYPE_TIMESTAMP:
                    sparkType = DataTypes.TimestampType;
                    omniTypes[i] = TimestampDataType.TIMESTAMP.getId().toValue();
                    omniVec = new LongVec(rowCount);
                    break;
                case VEC_TYPE_DATE64:
                    sparkType = DataTypes.DateType;
                    omniTypes[i] = Date64DataType.DATE64.getId().toValue();
                    omniVec = new LongVec(rowCount);
                    break;
                case VEC_TYPE_DECIMAL64:
                    sparkType = DataTypes.createDecimalType(protoTypeId.getPrecision(), protoTypeId.getScale());
                    omniTypes[i] = new Decimal64DataType(protoTypeId.getPrecision(), protoTypeId.getScale()).getId().toValue();
                    omniVec = new LongVec(rowCount);
                    break;
                case VEC_TYPE_SHORT:
                    sparkType = DataTypes.ShortType;
                    omniTypes[i] = ShortDataType.SHORT.getId().toValue();
                    omniVec = new ShortVec(rowCount);
                    break;
                case VEC_TYPE_BOOLEAN:
                    sparkType = DataTypes.BooleanType;
                    omniTypes[i] = BooleanDataType.BOOLEAN.getId().toValue();
                    omniVec = new BooleanVec(rowCount);
                    break;
                case VEC_TYPE_DOUBLE:
                    sparkType = DataTypes.DoubleType;
                    omniTypes[i] = DoubleDataType.DOUBLE.getId().toValue();
                    omniVec = new DoubleVec(rowCount);
                    break;
                case VEC_TYPE_VARCHAR:
                case VEC_TYPE_CHAR:
                    sparkType = DataTypes.StringType;
                    omniTypes[i] = VarcharDataType.VARCHAR.getId().toValue();
                    omniVec = new VarcharVec(rowCount); // it's capacity may be expanded.
                    break;
                case VEC_TYPE_DECIMAL128:
                    sparkType = DataTypes.createDecimalType(protoTypeId.getPrecision(), protoTypeId.getScale());
                    omniTypes[i] = new Decimal128DataType(protoTypeId.getPrecision(), protoTypeId.getScale()).getId().toValue();
                    omniVec = new Decimal128Vec(rowCount);
                    break;
                case VEC_TYPE_TIME32:
                case VEC_TYPE_TIME64:
                case VEC_TYPE_INTERVAL_DAY_TIME:
                case VEC_TYPE_INTERVAL_MONTHS:
                default:
                    throw new IllegalStateException("Unexpected value: " + protoTypeId.getTypeId());
            }

            omniVecs[i] = omniVec.getNativeVector();
            columnarVectors[i] = new OmniColumnVector(rowCount, sparkType, false);
            columnarVectors[i].setVec(omniVec);
        }
    }
}
