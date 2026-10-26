/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.api.state.serializer;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniSerializerJsonInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniStateMetaSerializerInfo;
import com.huawei.omniruntime.flink.runtime.metrics.exception.GeneralRuntimeException;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.ExternalSerializer;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/**
 * Unit tests for {@link OmniStateSerializerHelper}.
 */
public class OmniStateSerializerHelperTest {

    // ========== buildSerializerInfo tests ==========

    @Test
    @DisplayName("null serializerMap triggers IllegalArgumentException -> GeneralRuntimeException")
    void testBuildSerializerInfoWithNullSerializerMap() {
        // null serializerMap triggers IllegalArgumentException wrapped in GeneralRuntimeException
        assertThrows(GeneralRuntimeException.class, () ->
                OmniStateSerializerHelper.buildSerializerInfo(
                        "testState", 1, null,
                        new ExecutionConfig(), getClass().getClassLoader()));
    }

    @Test
    @DisplayName("empty serializerMap triggers IllegalArgumentException -> GeneralRuntimeException")
    void testBuildSerializerInfoWithEmptySerializerMap() {
        // empty serializerMap triggers IllegalArgumentException wrapped in GeneralRuntimeException
        assertThrows(GeneralRuntimeException.class, () ->
                OmniStateSerializerHelper.buildSerializerInfo(
                        "testState", 1, Collections.emptyMap(),
                        new ExecutionConfig(), getClass().getClassLoader()));
    }

    @Test
    @DisplayName("null ExecutionConfig triggers GeneralRuntimeException")
    void testBuildSerializerInfoWithNullExecutionConfig() {
        Map<String, String> serializerMap = new HashMap<>();
        serializerMap.put("keySerializer", "{}");

        assertThrows(GeneralRuntimeException.class, () ->
                OmniStateSerializerHelper.buildSerializerInfo(
                        "testState", 1, serializerMap, null, getClass().getClassLoader()));
    }

    @Test
    @DisplayName("null ClassLoader triggers GeneralRuntimeException")
    void testBuildSerializerInfoWithNullClassLoader() {
        Map<String, String> serializerMap = new HashMap<>();
        serializerMap.put("keySerializer", "{}");

        assertThrows(GeneralRuntimeException.class, () ->
                OmniStateSerializerHelper.buildSerializerInfo(
                        "testState", 1, serializerMap, new ExecutionConfig(), null));
    }

    @Test
    @DisplayName("invalid backendStateType code (999) triggers GeneralRuntimeException")
    void testBuildSerializerInfoWithInvalidBackendStateTypeCode() {
        Map<String, String> serializerMap = new HashMap<>();
        serializerMap.put("keySerializer", "{}");

        assertThrows(GeneralRuntimeException.class, () ->
                OmniStateSerializerHelper.buildSerializerInfo(
                        "testState", 999, serializerMap,
                        new ExecutionConfig(), getClass().getClassLoader()));
    }

    @Test
    @DisplayName("KEY_VALUE type with empty namespaceSerializer -> VoidNamespaceSerializer.INSTANCE")
    void testBuildSerializerInfoNamespaceSerializerEmptyValueSetsVoid() {
        Map<String, String> serializerMap = new HashMap<>();
        // BackendStateType.byCode(0) = KEY_VALUE, namespace serializer value empty
        // → should set VoidNamespaceSerializer.INSTANCE
        serializerMap.put("namespaceSerializer", "");

        OmniStateMetaSerializerInfo.Builder result = OmniStateSerializerHelper.buildSerializerInfo(
                "testState", 0, serializerMap,
                new ExecutionConfig(), getClass().getClassLoader());

        assertNotNull(result);
        OmniStateMetaSerializerInfo info = result.build();
        assertNotNull(info.getNamespaceSerializer());
        assertSame(VoidNamespaceSerializer.INSTANCE, info.getNamespaceSerializer());
    }

    @Test
    @DisplayName("serializerMap with null entries is safely skipped")
    void testBuildSerializerInfoWithSerializerMapContainingNullEntry() {
        Map<String, String> serializerMap = new HashMap<>();
        serializerMap.put(null, "value");
        serializerMap.put("validKey", "validValue");

        OmniStateMetaSerializerInfo.Builder result = OmniStateSerializerHelper.buildSerializerInfo(
                "testState", 1, serializerMap,
                new ExecutionConfig(), getClass().getClassLoader());

        assertNotNull(result);
    }

    @Test
    @DisplayName("unknown serializer key is skipped with a warning")
    void testBuildSerializerInfoWithUnknownSerializerKeySkipsGracefully() {
        Map<String, String> serializerMap = new HashMap<>();
        serializerMap.put("unknown_key_xyz", "someValue");

        OmniStateMetaSerializerInfo.Builder result = OmniStateSerializerHelper.buildSerializerInfo(
                "testState", 1, serializerMap,
                new ExecutionConfig(), getClass().getClassLoader());

        assertNotNull(result);
        OmniStateMetaSerializerInfo info = result.build();
        assertNotNull(info);
    }

    @Test
    @DisplayName("valid keySerializer JSON -> buildStateDescriptor returns descriptor -> serializerGroup populated")
    void testBuildSerializerInfoHappyPathWithKeySerializer() {
        Map<String, String> serializerMap = new HashMap<>();
        serializerMap.put("keySerializer", "{\"type\":8}"); // STRING type

        Map<String, Object> mockMap = new HashMap<>();
        mockMap.put("type", 8); // STRING

        try (MockedStatic<JsonHelper> jsonMock = mockStatic(JsonHelper.class)) {
            jsonMock.when(() -> JsonHelper.fromJson(anyString(), any(TypeReference.class)))
                    .thenReturn(mockMap);

            OmniStateMetaSerializerInfo.Builder result = OmniStateSerializerHelper.buildSerializerInfo(
                    "testState", 1, serializerMap,
                    new ExecutionConfig(), getClass().getClassLoader());

            assertNotNull(result);
            OmniStateMetaSerializerInfo info = result.build();
            // KEY_SERIALIZER meta key should be populated in the serializerGroup
            assertNotNull(info.getKeySerializer());
        }
    }

    // ========== buildStateDescriptor tests ==========

    @Test
    @DisplayName("empty JSON string returns null")
    void testBuildStateDescriptorWithEmptyJsonStr() {
        ExecutionConfig config = new ExecutionConfig();

        assertNull(OmniStateSerializerHelper.buildStateDescriptor(
                "testState", "key", "", config, getClass().getClassLoader()));
    }

    @Test
    @DisplayName("null JSON string returns null")
    void testBuildStateDescriptorWithNullJsonStr() {
        ExecutionConfig config = new ExecutionConfig();

        assertNull(OmniStateSerializerHelper.buildStateDescriptor(
                "testState", "key", null, config, getClass().getClassLoader()));
    }

    @Test
    @DisplayName("null ClassLoader returns null")
    void testBuildStateDescriptorWithNullClassLoader() {
        ExecutionConfig config = new ExecutionConfig();
        String validJson = "{\"type\":0}";

        assertNull(OmniStateSerializerHelper.buildStateDescriptor(
                "testState", "key", validJson, config, null));
    }

    @Test
    @DisplayName("null ExecutionConfig returns null")
    void testBuildStateDescriptorWithNullExecutionConfig() {
        String validJson = "{\"type\":0}";

        assertNull(OmniStateSerializerHelper.buildStateDescriptor(
                "testState", "key", validJson, null, getClass().getClassLoader()));
    }

    @Test
    @DisplayName("valid STRING type JSON -> builds ValueStateDescriptor via convert/OmniParseFactory")
    void testBuildStateDescriptorHappyPathWithStringSerializer() {
        ExecutionConfig config = new ExecutionConfig();
        Map<String, Object> mockMap = new HashMap<>();
        mockMap.put("type", 8); // STRING

        try (MockedStatic<JsonHelper> jsonMock = mockStatic(JsonHelper.class)) {
            jsonMock.when(() -> JsonHelper.fromJson(anyString(), any(TypeReference.class)))
                    .thenReturn(mockMap);

            StateDescriptor<?, ?> result = OmniStateSerializerHelper.buildStateDescriptor(
                    "testState", "key", "{\"type\":8}", config, getClass().getClassLoader());

            assertNotNull(result);
        }
    }

    @Test
    @DisplayName("legacy POJO JSON without serializerAttributes builds descriptor")
    void testBuildStateDescriptorLegacyPojoWithoutSerializerAttributes() {
        ExecutionConfig config = new ExecutionConfig();
        String legacyJson = "{\"type\":7,\"element_type\":\"" + LegacyPojo.class.getName() + "\"}";

        StateDescriptor<?, ?> result = OmniStateSerializerHelper.buildStateDescriptor(
                "testState", "key", legacyJson, config, getClass().getClassLoader());

        assertNotNull(result);
    }

    @Test
    @DisplayName("legacy TUPLE JSON without serializerAttributes builds descriptor")
    void testBuildStateDescriptorLegacyTupleWithoutSerializerAttributes() {
        ExecutionConfig config = new ExecutionConfig();
        String legacyJson = "{\"type\":13,\"element_type\":\"org.apache.flink.api.java.tuple.Tuple2\","
                + "\"fieldSerializers\":[\"{\\\"type\\\":8}\",\"{\\\"type\\\":4}\"]}";

        StateDescriptor<?, ?> result = OmniStateSerializerHelper.buildStateDescriptor(
                "testState", "key", legacyJson, config, getClass().getClassLoader());

        assertNotNull(result);
    }

    // ========== buildSerializerJsonInfo tests ==========

    @Test
    @DisplayName("null StateMetaInfoSnapshot returns empty map")
    void testBuildSerializerJsonInfoWithNullMetaInfo() {
        Map<String, Object> result = OmniStateSerializerHelper.buildSerializerJsonInfo(null);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("empty snapshot returns map with serializer and keySerializer keys")
    void testBuildSerializerJsonInfoWithEmptySerializerSnapshot() {
        StateMetaInfoSnapshot snapshot = new StateMetaInfoSnapshot(
                "testState",
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap());

        Map<String, Object> result = OmniStateSerializerHelper.buildSerializerJsonInfo(snapshot);

        assertNotNull(result);
        assertTrue(result.containsKey("serializer"));
        assertTrue(result.containsKey("keySerializer"));
    }

    // ========== buildJsonInfo tests ==========

    @Test
    @DisplayName("null TypeSerializer returns null")
    void testBuildJsonInfoWithNullTypeSerializer() {
        assertNull(OmniStateSerializerHelper.buildJsonInfo(null));
    }

    @Test
    @DisplayName("IntSerializer produces non-null OmniSerializerJsonInfo")
    void testBuildJsonInfoWithIntSerializer() {
        TypeSerializer<?> serializer = IntSerializer.INSTANCE;

        OmniSerializerJsonInfo result = OmniStateSerializerHelper.buildJsonInfo(serializer);

        assertNotNull(result);
    }

    @Test
    @DisplayName("StringSerializer produces non-null OmniSerializerJsonInfo")
    void testBuildJsonInfoWithStringSerializer() {
        TypeSerializer<?> serializer = StringSerializer.INSTANCE;

        OmniSerializerJsonInfo result = OmniStateSerializerHelper.buildJsonInfo(serializer);

        assertNotNull(result);
    }

    @Test
    @DisplayName("ExternalSerializer round-trip preserves conversion class")
    void testExternalSerializerRoundTripPreservesConversionClass() {
        DataType dataType = DataTypes.ROW(
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("count", DataTypes.INT()))
                .bridgedTo(RowData.class);
        TypeSerializer<?> serializer = ExternalTypeInfo.of(dataType, false).createSerializer(new ExecutionConfig());
        OmniSerializerJsonInfo jsonInfo = OmniStateSerializerHelper.buildJsonInfo(serializer);
        String json = "{\"type\":17,"
                + "\"logicalType\":" + JsonHelper.toJson(jsonInfo.getLogicalType()) + ","
                + "\"serializerAttributes\":" + JsonHelper.toJson(jsonInfo.getSerializerAttributes()) + "}";

        StateDescriptor<?, ?> result = OmniStateSerializerHelper.buildStateDescriptor(
                "testState", "value", json, new ExecutionConfig(), getClass().getClassLoader());
        TypeSerializer<?> restoredSerializer = result.getSerializer();

        assertTrue(restoredSerializer instanceof ExternalSerializer);
        assertSame(RowData.class, ((ExternalSerializer<?, ?>) restoredSerializer).getDataType().getConversionClass());
    }

    @Test
    @DisplayName("TypeSerializer with no matching OmniSerializerType -> returns null")
    void testBuildJsonInfoWithUnknownSerializerClass() {
        // Mockito creates a synthetic class not registered in OmniSerializerType enum
        TypeSerializer<?> customSerializer = mock(TypeSerializer.class);

        assertNull(OmniStateSerializerHelper.buildJsonInfo(customSerializer));
    }

    // ========== getStateBackendKeySerializer tests ==========

    @Test
    @DisplayName("null keySerializer value in metaInfo throws exception")
    void testGetStateBackendKeySerializerMetaInfoWithNullKeySerializerValue() {
        Map<String, Object> metaInfo = new HashMap<>();
        metaInfo.put("name", "testState");
        metaInfo.put("keySerializer", null);

        assertThrows(Exception.class, () ->
                OmniStateSerializerHelper.getStateBackendKeySerializer(
                        metaInfo, new ExecutionConfig(), getClass().getClassLoader()));
    }

    @Test
    @DisplayName("valid keySerializer JSON -> builds state descriptor and returns its serializer")
    void testGetStateBackendKeySerializerMetaInfoHappyPath() {
        Map<String, Object> metaInfo = new HashMap<>();
        metaInfo.put("name", "testState");
        metaInfo.put("keySerializer", "{\"type\":8}"); // STRING serializer JSON

        Map<String, Object> mockMap = new HashMap<>();
        mockMap.put("type", 8); // STRING

        try (MockedStatic<JsonHelper> jsonMock = mockStatic(JsonHelper.class)) {
            jsonMock.when(() -> JsonHelper.fromJson(anyString(), any(TypeReference.class)))
                    .thenReturn(mockMap);

            TypeSerializer<?> result = OmniStateSerializerHelper.getStateBackendKeySerializer(
                    metaInfo, new ExecutionConfig(), getClass().getClassLoader());

            assertNotNull(result);
        }
    }

    @Test
    @DisplayName("UNKNOWN type keySerializer JSON -> buildStateDescriptor returns null -> getStateBackendKeySerializer returns null")
    void testGetStateBackendKeySerializerMetaInfoNullStateDescriptor() {
        Map<String, Object> metaInfo = new HashMap<>();
        metaInfo.put("name", "testState");
        metaInfo.put("keySerializer", "{\"type\":0}"); // UNKNOWN type has no factory

        Map<String, Object> mockMap = new HashMap<>();
        mockMap.put("type", 0); // UNKNOWN

        try (MockedStatic<JsonHelper> jsonMock = mockStatic(JsonHelper.class)) {
            jsonMock.when(() -> JsonHelper.fromJson(anyString(), any(TypeReference.class)))
                    .thenReturn(mockMap);

            TypeSerializer<?> result = OmniStateSerializerHelper.getStateBackendKeySerializer(
                    metaInfo, new ExecutionConfig(), getClass().getClassLoader());

            assertNull(result);
        }
    }

    @Test
    @DisplayName("null StreamTask returns null")
    void testGetStateBackendKeySerializerStreamTaskWithNull() {
        assertNull(OmniStateSerializerHelper.getStateBackendKeySerializer((StreamTask<?, ?>) null));
    }

    @Test
    @DisplayName("StreamTask without mainOperator returns null")
    void testGetStateBackendKeySerializerStreamTaskWithNoMainOperator() {
        try (MockedStatic<ReflectionUtils> reflectionMock = mockStatic(ReflectionUtils.class)) {
            @SuppressWarnings("unchecked")
            StreamTask<?, ?> streamTask = mock(StreamTask.class);
            reflectionMock.when(() -> ReflectionUtils.retrievePrivateField(
                            eq(streamTask), eq("mainOperator")))
                    .thenReturn(null);

            assertNull(OmniStateSerializerHelper.getStateBackendKeySerializer(streamTask));
        }
    }

    @Test
    @DisplayName("StreamTask with operator but no keyedBackend returns null")
    void testGetStateBackendKeySerializerStreamTaskWithOperatorNoKeyedBackend() {
        try (MockedStatic<ReflectionUtils> reflectionMock = mockStatic(ReflectionUtils.class)) {
            @SuppressWarnings("unchecked")
            StreamTask<?, ?> streamTask = mock(StreamTask.class);
            AbstractStreamOperator<?> operator = mock(AbstractStreamOperator.class);
            reflectionMock.when(() -> ReflectionUtils.retrievePrivateField(
                            eq(streamTask), eq("mainOperator")))
                    .thenReturn(operator);

            assertNull(OmniStateSerializerHelper.getStateBackendKeySerializer(streamTask));
        }
    }

    public static class LegacyPojo {
        public String name;
        public int count;
    }
}
