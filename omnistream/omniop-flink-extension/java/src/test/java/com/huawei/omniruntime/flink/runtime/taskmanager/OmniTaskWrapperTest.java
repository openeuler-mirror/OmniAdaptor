/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.taskmanager;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.OmniStateSerializerHelper;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniStateMetaSerializerInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.utils.OmniStateSerializerUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.taskmanager.RuntimeEnvironment;
import org.apache.flink.util.UserCodeClassLoader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link OmniTaskWrapper}, covering savepoint output stream operations
 * and savepoint metadata writing.
 */
public class OmniTaskWrapperTest {

    private static final long CHECKPOINT_ID = 42L;

    private static final String VALID_CHECKPOINT_OPTION_JSON =
            "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"ALIGNED\","
            + "\"checkpointType\":{\"name\":\"Checkpoint\"}}";

    private static final String VALID_SAVEPOINT_OPTION_JSON =
            "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"ALIGNED\","
            + "\"checkpointType\":{\"name\":\"Savepoint\",\"formatType\":0},"
            + "\"targetLocation\":{\"referenceBytes\":\"test-ref-bytes\"}}";

    @Mock
    private OmniTask omniTask;

    @Mock
    private CheckpointStreamWithResultProvider provider;

    @Mock
    private CheckpointStreamWithResultProvider acquiredProvider;

    @Mock
    private SnapshotResult<StreamStateHandle> snapshotResult;

    @Mock
    private ExecutionConfig executionConfig;

    @Mock
    private RuntimeEnvironment runtimeEnvironment;

    @Mock
    private UserCodeClassLoader userCodeClassLoader;

    @Mock
    private JobID jobId;

    private OmniTaskWrapper wrapper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        wrapper = new OmniTaskWrapper(omniTask);
    }

    // ========== acquireSavepointOutputStream tests ==========

    @Test
    @DisplayName("valid Checkpoint-type option JSON -> delegates to omniTask")
    void testAcquireSavepointOutputStreamWithValidCheckpointOption() throws Exception {
        when(omniTask.acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class)))
                .thenReturn(acquiredProvider);

        CheckpointStreamWithResultProvider result =
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, VALID_CHECKPOINT_OPTION_JSON);

        assertNotNull(result);
        assertSame(acquiredProvider, result);
        verify(omniTask).acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class));
    }

    @Test
    @DisplayName("valid Savepoint-type option JSON -> delegates to omniTask")
    void testAcquireSavepointOutputStreamWithValidSavepointOption() throws Exception {
        when(omniTask.acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class)))
                .thenReturn(acquiredProvider);

        CheckpointStreamWithResultProvider result =
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, VALID_SAVEPOINT_OPTION_JSON);

        assertNotNull(result);
        verify(omniTask).acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class));
    }

    @Test
    @DisplayName("invalid JSON throws exception, omniTask.acquireSavepointOutputStream never called")
    void testAcquireSavepointOutputStreamWithInvalidJsonThrowsException() throws Exception {
        String invalidJson = "{invalid}";

        assertThrows(Exception.class, () ->
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, invalidJson));
        verify(omniTask, never()).acquireSavepointOutputStream(anyLong(), any());
    }

    @Test
    @DisplayName("omniTask exception is propagated without wrapping")
    void testAcquireSavepointOutputStreamPropagatesOmniTaskException() throws Exception {
        when(omniTask.acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class)))
                .thenThrow(new IOException("Storage error"));

        Exception ex = assertThrows(Exception.class, () ->
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, VALID_CHECKPOINT_OPTION_JSON));

        assertTrue(ex.getMessage().contains("Storage error"));
    }

    @Test
    @DisplayName("missing alignmentType field -> IllegalArgumentException")
    void testParseCheckpointOptionsMissingAlignmentType() {
        String json = "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":null,"
                + "\"checkpointType\":{\"name\":\"Checkpoint\"}}";

        Exception ex = assertThrows(Exception.class, () ->
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, json));

        assertTrue(ex.getMessage().contains("alignmentType is required"));
    }

    @Test
    @DisplayName("empty alignmentType field -> IllegalArgumentException")
    void testParseCheckpointOptionsEmptyAlignmentType() {
        String json = "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"\","
                + "\"checkpointType\":{\"name\":\"Checkpoint\"}}";

        Exception ex = assertThrows(Exception.class, () ->
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, json));

        assertTrue(ex.getMessage().contains("alignmentType is required"));
    }

    @Test
    @DisplayName("missing checkpointType field -> IllegalArgumentException")
    void testParseCheckpointOptionsMissingCheckpointType() {
        String json = "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"ALIGNED\"}";

        Exception ex = assertThrows(Exception.class, () ->
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, json));

        assertTrue(ex.getMessage().contains("Missing required field: checkpointType"));
    }

    @Test
    @DisplayName("missing checkpointType.name field -> IllegalArgumentException")
    void testParseCheckpointOptionsMissingCheckpointTypeName() {
        String json = "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"ALIGNED\","
                + "\"checkpointType\":{\"name\":null}}";

        Exception ex = assertThrows(Exception.class, () ->
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, json));

        assertTrue(ex.getMessage().contains("Missing required field: checkpointType name"));
    }

    @Test
    @DisplayName("alignmentType AT_LEAST_ONCE -> isExactlyOnceMode=true")
    void testParseCheckpointOptionsAtLeastOnce() throws Exception {
        when(omniTask.acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class)))
                .thenReturn(acquiredProvider);

        String json = "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"AT_LEAST_ONCE\","
                + "\"checkpointType\":{\"name\":\"Checkpoint\"}}";

        CheckpointStreamWithResultProvider result =
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, json);

        assertNotNull(result);
        verify(omniTask).acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class));
    }

    @Test
    @DisplayName("alignmentType UNALIGNED -> isUnalignedEnabled=true")
    void testParseCheckpointOptionsUnaligned() throws Exception {
        when(omniTask.acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class)))
                .thenReturn(acquiredProvider);

        String json = "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"UNALIGNED\","
                + "\"checkpointType\":{\"name\":\"Checkpoint\"}}";

        CheckpointStreamWithResultProvider result =
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, json);

        assertNotNull(result);
        verify(omniTask).acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class));
    }

    @Test
    @DisplayName("Savepoint formatType=1 -> savepoint(NATIVE)")
    void testParseCheckpointOptionsSavepointNative() throws Exception {
        when(omniTask.acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class)))
                .thenReturn(acquiredProvider);

        String json = "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"ALIGNED\","
                + "\"checkpointType\":{\"name\":\"Savepoint\",\"formatType\":1},"
                + "\"targetLocation\":{\"referenceBytes\":\"test-ref-bytes\"}}";

        CheckpointStreamWithResultProvider result =
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, json);

        assertNotNull(result);
        verify(omniTask).acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class));
    }

    @Test
    @DisplayName("Terminate Savepoint formatType=0 -> terminate(CANONICAL)")
    void testParseCheckpointOptionsTerminateSavepointCanonical() throws Exception {
        when(omniTask.acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class)))
                .thenReturn(acquiredProvider);

        String json = "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"ALIGNED\","
                + "\"checkpointType\":{\"name\":\"Terminate Savepoint\",\"formatType\":0},"
                + "\"targetLocation\":{\"referenceBytes\":\"test-ref-bytes\"}}";

        CheckpointStreamWithResultProvider result =
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, json);

        assertNotNull(result);
        verify(omniTask).acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class));
    }

    @Test
    @DisplayName("Terminate Savepoint formatType=1 -> terminate(NATIVE)")
    void testParseCheckpointOptionsTerminateSavepointNative() throws Exception {
        when(omniTask.acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class)))
                .thenReturn(acquiredProvider);

        String json = "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"ALIGNED\","
                + "\"checkpointType\":{\"name\":\"Terminate Savepoint\",\"formatType\":1},"
                + "\"targetLocation\":{\"referenceBytes\":\"test-ref-bytes\"}}";

        CheckpointStreamWithResultProvider result =
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, json);

        assertNotNull(result);
        verify(omniTask).acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class));
    }

    @Test
    @DisplayName("Suspend Savepoint formatType=0 -> suspend(CANONICAL)")
    void testParseCheckpointOptionsSuspendSavepointCanonical() throws Exception {
        when(omniTask.acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class)))
                .thenReturn(acquiredProvider);

        String json = "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"ALIGNED\","
                + "\"checkpointType\":{\"name\":\"Suspend Savepoint\",\"formatType\":0},"
                + "\"targetLocation\":{\"referenceBytes\":\"test-ref-bytes\"}}";

        CheckpointStreamWithResultProvider result =
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, json);

        assertNotNull(result);
        verify(omniTask).acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class));
    }

    @Test
    @DisplayName("Suspend Savepoint formatType=1 -> suspend(NATIVE)")
    void testParseCheckpointOptionsSuspendSavepointNative() throws Exception {
        when(omniTask.acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class)))
                .thenReturn(acquiredProvider);

        String json = "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"ALIGNED\","
                + "\"checkpointType\":{\"name\":\"Suspend Savepoint\",\"formatType\":1},"
                + "\"targetLocation\":{\"referenceBytes\":\"test-ref-bytes\"}}";

        CheckpointStreamWithResultProvider result =
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, json);

        assertNotNull(result);
        verify(omniTask).acquireSavepointOutputStream(eq(CHECKPOINT_ID), any(CheckpointOptions.class));
    }

    @Test
    @DisplayName("savepoint path missing targetLocation -> IllegalArgumentException")
    void testParseCheckpointOptionsMissingTargetLocation() {
        String json = "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"ALIGNED\","
                + "\"checkpointType\":{\"name\":\"Savepoint\",\"formatType\":0}}";

        Exception ex = assertThrows(Exception.class, () ->
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, json));

        assertTrue(ex.getMessage().contains("Missing required field: targetLocation"));
    }

    @Test
    @DisplayName("savepoint path missing referenceBytes -> IllegalArgumentException")
    void testParseCheckpointOptionsMissingReferenceBytes() {
        String json = "{\"alignedCheckpointTimeout\":10000,\"alignmentType\":\"ALIGNED\","
                + "\"checkpointType\":{\"name\":\"Savepoint\",\"formatType\":0},"
                + "\"targetLocation\":{\"referenceBytes\":\"\"}}";

        Exception ex = assertThrows(Exception.class, () ->
                wrapper.acquireSavepointOutputStream(CHECKPOINT_ID, json));

        assertTrue(ex.getMessage().contains("targetLocation.referenceBytes is required"));
    }

    // ========== writeSavepointMetadata tests ==========

    @Test
    @DisplayName("single state: JSON parsed -> StateMetaInfoSnapshot built -> delegated with keySerializer")
    void testWriteSavepointMetadataHappyPath() throws Exception {
        String stateMetaInfoJson = "[{\"name\":\"testState\",\"backendStateType\":1,"
                + "\"options\":{\"keyedStateType\":\"VALUE\"},"
                + "\"serializer\":\"{}\"}]";

        when(omniTask.getExecutionConfig()).thenReturn(executionConfig);
        when(omniTask.getCheckpointingEnv()).thenReturn(runtimeEnvironment);
        when(runtimeEnvironment.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        when(userCodeClassLoader.asClassLoader()).thenReturn(getClass().getClassLoader());

        OmniStateMetaSerializerInfo.Builder mockBuilder = mock(OmniStateMetaSerializerInfo.Builder.class);
        OmniStateMetaSerializerInfo mockSerializerInfo = mock(OmniStateMetaSerializerInfo.class);
        when(mockBuilder.build()).thenReturn(mockSerializerInfo);
        when(mockSerializerInfo.getSerializerSnapshotGroup()).thenReturn(Collections.emptyMap());
        when(mockSerializerInfo.getSerializerGroup()).thenReturn(Collections.emptyMap());

        TypeSerializer<?> keySerializer = StringSerializer.INSTANCE;

        try (MockedStatic<OmniStateSerializerHelper> helperMock =
                     mockStatic(OmniStateSerializerHelper.class);
             MockedStatic<JsonHelper> jsonHelperMock = mockStatic(JsonHelper.class)) {

            jsonHelperMock.when(() -> JsonHelper.fromJson(anyString(), eq(HashMap.class)))
                    .thenReturn(new HashMap<>());

            helperMock.when(() -> OmniStateSerializerHelper.getStateBackendKeySerializer(
                            anyMap(), eq(executionConfig), any()))
                    .thenReturn((TypeSerializer) keySerializer);

            helperMock.when(() -> OmniStateSerializerHelper.buildSerializerInfo(
                            eq("testState"), eq(1), anyMap(), eq(executionConfig), any()))
                    .thenReturn(mockBuilder);

            wrapper.writeSavepointMetadata(provider, stateMetaInfoJson);

            verify(omniTask).writeSavepointMetadata(eq(provider), anyList(), eq(keySerializer));
        }
    }

    @Test
    @DisplayName("multiple states: all parsed and delegated together")
    void testWriteSavepointMetadataWithMultipleStates() throws Exception {
        String stateMetaInfoJson = "["
                + "{\"name\":\"state1\",\"backendStateType\":1,\"options\":{\"keyedStateType\":\"VALUE\"},\"serializer\":\"{}\"},"
                + "{\"name\":\"state2\",\"backendStateType\":2,\"options\":{\"keyedStateType\":\"LIST\"},\"serializer\":\"{}\"}"
                + "]";

        when(omniTask.getExecutionConfig()).thenReturn(executionConfig);
        when(omniTask.getCheckpointingEnv()).thenReturn(runtimeEnvironment);
        when(runtimeEnvironment.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        when(userCodeClassLoader.asClassLoader()).thenReturn(getClass().getClassLoader());

        OmniStateMetaSerializerInfo.Builder mockBuilder = mock(OmniStateMetaSerializerInfo.Builder.class);
        OmniStateMetaSerializerInfo mockSerializerInfo = mock(OmniStateMetaSerializerInfo.class);
        when(mockBuilder.build()).thenReturn(mockSerializerInfo);
        when(mockSerializerInfo.getSerializerSnapshotGroup()).thenReturn(Collections.emptyMap());
        when(mockSerializerInfo.getSerializerGroup()).thenReturn(Collections.emptyMap());

        TypeSerializer<?> keySerializer = StringSerializer.INSTANCE;

        try (MockedStatic<OmniStateSerializerHelper> helperMock =
                     mockStatic(OmniStateSerializerHelper.class);
             MockedStatic<JsonHelper> jsonHelperMock = mockStatic(JsonHelper.class)) {

            jsonHelperMock.when(() -> JsonHelper.fromJson(anyString(), eq(HashMap.class)))
                    .thenReturn(new HashMap<>());

            helperMock.when(() -> OmniStateSerializerHelper.getStateBackendKeySerializer(
                            anyMap(), eq(executionConfig), any()))
                    .thenReturn((TypeSerializer) keySerializer);

            helperMock.when(() -> OmniStateSerializerHelper.buildSerializerInfo(
                            anyString(), anyInt(), anyMap(), eq(executionConfig), any()))
                    .thenReturn(mockBuilder);

            wrapper.writeSavepointMetadata(provider, stateMetaInfoJson);

            verify(omniTask).writeSavepointMetadata(eq(provider), anyList(), eq(keySerializer));
        }
    }

    @Test
    @DisplayName("invalid JSON -> IOException with message Failed to writeSavepoint metadata")
    void testWriteSavepointMetadataWithInvalidJsonThrowsIOException() throws Exception {
        String invalidJson = "not valid json";

        Exception ex = assertThrows(Exception.class, () ->
                wrapper.writeSavepointMetadata(provider, invalidJson));

        assertTrue(ex instanceof IOException);
        assertTrue(ex.getMessage().contains("Failed to writeSavepoint metadata"));
        verify(omniTask, never()).writeSavepointMetadata(any(), anyList(), any());
    }

    @Test
    @DisplayName("internal RuntimeException wrapped in IOException")
    void testWriteSavepointMetadataWrapExceptionInIOException() throws Exception {
        String stateMetaInfoJson = "[{\"name\":\"testState\",\"backendStateType\":1,"
                + "\"options\":{\"keyedStateType\":\"VALUE\"},"
                + "\"serializer\":\"{}\"}]";

        when(omniTask.getExecutionConfig()).thenReturn(executionConfig);
        when(omniTask.getCheckpointingEnv()).thenThrow(new RuntimeException("Env error"));

        Exception ex = assertThrows(Exception.class, () ->
                wrapper.writeSavepointMetadata(provider, stateMetaInfoJson));

        assertTrue(ex instanceof IOException);
        assertTrue(ex.getMessage().contains("Failed to writeSavepoint metadata"));
        verify(omniTask, never()).writeSavepointMetadata(any(), anyList(), any());
    }

    @Test
    @DisplayName("null keySerializer from helper passed as null to omniTask.writeSavepointMetadata")
    void testWriteSavepointMetadataWithNullKeySerializer() throws Exception {
        String stateMetaInfoJson = "[{\"name\":\"testState\",\"backendStateType\":1,"
                + "\"options\":{\"keyedStateType\":\"VALUE\"},"
                + "\"serializer\":\"{}\"}]";

        when(omniTask.getExecutionConfig()).thenReturn(executionConfig);
        when(omniTask.getCheckpointingEnv()).thenReturn(runtimeEnvironment);
        when(runtimeEnvironment.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        when(userCodeClassLoader.asClassLoader()).thenReturn(getClass().getClassLoader());
        OmniStateMetaSerializerInfo.Builder mockBuilder = mock(OmniStateMetaSerializerInfo.Builder.class);
        OmniStateMetaSerializerInfo mockSerializerInfo = mock(OmniStateMetaSerializerInfo.class);
        when(mockBuilder.build()).thenReturn(mockSerializerInfo);
        when(mockSerializerInfo.getSerializerSnapshotGroup()).thenReturn(Collections.emptyMap());
        when(mockSerializerInfo.getSerializerGroup()).thenReturn(Collections.emptyMap());

        try (MockedStatic<OmniStateSerializerHelper> helperMock =
                     mockStatic(OmniStateSerializerHelper.class);
             MockedStatic<JsonHelper> jsonHelperMock = mockStatic(JsonHelper.class)) {

            jsonHelperMock.when(() -> JsonHelper.fromJson(anyString(), eq(HashMap.class)))
                    .thenReturn(new HashMap<>());

            // keySerializer returns null from static helper
            helperMock.when(() -> OmniStateSerializerHelper.getStateBackendKeySerializer(
                            anyMap(), eq(executionConfig), any()))
                    .thenReturn(null);

            helperMock.when(() -> OmniStateSerializerHelper.buildSerializerInfo(
                            eq("testState"), eq(1), anyMap(), eq(executionConfig), any()))
                    .thenReturn(mockBuilder);

            wrapper.writeSavepointMetadata(provider, stateMetaInfoJson);

            // keySerializer is null but the loop only calls getStateBackendKeySerializer when null,
            // so the result is passed as null to omniTask.writeSavepointMetadata
            verify(omniTask).writeSavepointMetadata(eq(provider), anyList(), eq(null));
        }
    }

    // ========== writeOperatorMetaData tests ==========

    private static final String EMPTY_OPERATOR_STATE_JSON = "[]";
    private static final String EMPTY_BROADCAST_STATE_JSON = "[]";

    @Test
    @DisplayName("empty operator/broadcast state: builds metadata bytes -> delegates to writeOperatorMetaDataBytes")
    void testWriteOperatorMetaDataHappyPath() throws Exception {
        when(omniTask.getCheckpointingEnv()).thenReturn(runtimeEnvironment);
        when(runtimeEnvironment.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        when(userCodeClassLoader.asClassLoader()).thenReturn(getClass().getClassLoader());
        when(omniTask.getJobID()).thenReturn(jobId);

        try (MockedStatic<JsonHelper> jsonHelperMock = mockStatic(JsonHelper.class);
             MockedStatic<OmniStateSerializerUtils> utilsMock = mockStatic(OmniStateSerializerUtils.class)) {

            jsonHelperMock.when(() -> JsonHelper.fromJson(eq(EMPTY_OPERATOR_STATE_JSON), any(TypeReference.class)))
                    .thenReturn(Collections.emptyList());
            jsonHelperMock.when(() -> JsonHelper.fromJson(eq(EMPTY_BROADCAST_STATE_JSON), any(TypeReference.class)))
                    .thenReturn(Collections.emptyList());

            utilsMock.when(() -> OmniStateSerializerUtils.buildStateMetaInfoSnapshot(
                            eq(omniTask), anyList()))
                    .thenReturn(Collections.emptyList());

            wrapper.writeOperatorMetaData(provider,
                    EMPTY_OPERATOR_STATE_JSON, EMPTY_BROADCAST_STATE_JSON);

            verify(omniTask).writeOperatorMetaDataBytes(eq(provider), any(byte[].class));
        }
    }

    @Test
    @DisplayName("second call with same params hits cache, both calls succeed")
    void testWriteOperatorMetaDataWithCachedResult() throws Exception {
        when(omniTask.getCheckpointingEnv()).thenReturn(runtimeEnvironment);
        when(runtimeEnvironment.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        when(userCodeClassLoader.asClassLoader()).thenReturn(getClass().getClassLoader());
        when(omniTask.getJobID()).thenReturn(jobId);

        try (MockedStatic<JsonHelper> jsonHelperMock = mockStatic(JsonHelper.class);
             MockedStatic<OmniStateSerializerUtils> utilsMock = mockStatic(OmniStateSerializerUtils.class)) {

            jsonHelperMock.when(() -> JsonHelper.fromJson(eq(EMPTY_OPERATOR_STATE_JSON), any(TypeReference.class)))
                    .thenReturn(Collections.emptyList());
            jsonHelperMock.when(() -> JsonHelper.fromJson(eq(EMPTY_BROADCAST_STATE_JSON), any(TypeReference.class)))
                    .thenReturn(Collections.emptyList());

            utilsMock.when(() -> OmniStateSerializerUtils.buildStateMetaInfoSnapshot(
                            eq(omniTask), anyList()))
                    .thenReturn(Collections.emptyList());

            // Call twice: second call should hit cache
            wrapper.writeOperatorMetaData(provider,
                    EMPTY_OPERATOR_STATE_JSON, EMPTY_BROADCAST_STATE_JSON);
            wrapper.writeOperatorMetaData(provider,
                    EMPTY_OPERATOR_STATE_JSON, EMPTY_BROADCAST_STATE_JSON);

            verify(omniTask, times(2)).writeOperatorMetaDataBytes(eq(provider), any(byte[].class));
        }
    }

    @Test
    @DisplayName("JSON parse error -> IOException, writeOperatorMetaDataBytes never called")
    void testWriteOperatorMetaDataThrowsIOExceptionOnException() throws Exception {
        when(omniTask.getCheckpointingEnv()).thenReturn(runtimeEnvironment);
        when(runtimeEnvironment.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        when(userCodeClassLoader.asClassLoader()).thenReturn(getClass().getClassLoader());
        when(omniTask.getJobID()).thenReturn(jobId);

        try (MockedStatic<JsonHelper> jsonHelperMock = mockStatic(JsonHelper.class)) {
            jsonHelperMock.when(() -> JsonHelper.fromJson(eq(EMPTY_OPERATOR_STATE_JSON), any(TypeReference.class)))
                    .thenThrow(new RuntimeException("JSON parse error"));

            Exception ex = assertThrows(IOException.class, () ->
                    wrapper.writeOperatorMetaData(provider,
                            EMPTY_OPERATOR_STATE_JSON, EMPTY_BROADCAST_STATE_JSON));

            assertTrue(ex.getMessage().contains("Failed to materialize operator metadata"));
            verify(omniTask, never()).writeOperatorMetaDataBytes(any(), any());
        }
    }

    @Test
    @DisplayName("metadata build OK but writeOperatorMetaDataBytes fails -> IOException")
    void testWriteOperatorMetaDataPropagatesWriteException() throws Exception {
        when(omniTask.getCheckpointingEnv()).thenReturn(runtimeEnvironment);
        when(runtimeEnvironment.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        when(userCodeClassLoader.asClassLoader()).thenReturn(getClass().getClassLoader());
        when(omniTask.getJobID()).thenReturn(jobId);

        try (MockedStatic<JsonHelper> jsonHelperMock = mockStatic(JsonHelper.class);
             MockedStatic<OmniStateSerializerUtils> utilsMock = mockStatic(OmniStateSerializerUtils.class)) {

            jsonHelperMock.when(() -> JsonHelper.fromJson(eq(EMPTY_OPERATOR_STATE_JSON), any(TypeReference.class)))
                    .thenReturn(Collections.emptyList());
            jsonHelperMock.when(() -> JsonHelper.fromJson(eq(EMPTY_BROADCAST_STATE_JSON), any(TypeReference.class)))
                    .thenReturn(Collections.emptyList());

            utilsMock.when(() -> OmniStateSerializerUtils.buildStateMetaInfoSnapshot(
                            eq(omniTask), anyList()))
                    .thenReturn(Collections.emptyList());

            doThrow(new RuntimeException("Write error")).when(omniTask)
                    .writeOperatorMetaDataBytes(eq(provider), any(byte[].class));

            Exception ex = assertThrows(IOException.class, () ->
                    wrapper.writeOperatorMetaData(provider,
                            EMPTY_OPERATOR_STATE_JSON, EMPTY_BROADCAST_STATE_JSON));

            assertTrue(ex.getMessage().contains("Failed to materialize operator metadata"));
            verify(omniTask).writeOperatorMetaDataBytes(eq(provider), any(byte[].class));
        }
    }

    @Test
    @DisplayName("non-empty operator state with LONG serializer -> fast path bypasses buildStateMetaInfoSnapshot")
    void testWriteOperatorMetaDataFastPathWithLongSerializer() throws Exception {
        String nonEmptyOpJson = "[{\"name\":\"fastOp\",\"backendStateType\":1,"
                + "\"options\":{},\"serializer\":\"fast-serializer-obj\"}]";

        Map<String, Object> operatorMap = new HashMap<>();
        operatorMap.put("name", "fastOp");
        operatorMap.put("backendStateType", 1); // OPERATOR
        operatorMap.put("serializer", "fast-serializer-obj");
        operatorMap.put("options", new HashMap<>());

        Map<String, String> fastSerializerMap = new HashMap<>();
        fastSerializerMap.put("stateSerializer", "{\"type\":3}"); // LONG

        Map<String, Object> fastSerializerTypeMap = new HashMap<>();
        fastSerializerTypeMap.put("type", 3); // LONG

        when(omniTask.getCheckpointingEnv()).thenReturn(runtimeEnvironment);
        when(runtimeEnvironment.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        when(userCodeClassLoader.asClassLoader()).thenReturn(getClass().getClassLoader());
        when(omniTask.getJobID()).thenReturn(jobId);

        try (MockedStatic<JsonHelper> jsonHelperMock = mockStatic(JsonHelper.class)) {
            // Call 1: buildOperatorMetadata parses operator state JSON
            jsonHelperMock.when(() -> JsonHelper.fromJson(eq(nonEmptyOpJson), any(TypeReference.class)))
                    .thenReturn(Collections.singletonList(operatorMap));
            // Call 2: buildOperatorMetadata parses broadcast state JSON (empty)
            jsonHelperMock.when(() -> JsonHelper.fromJson(eq(EMPTY_BROADCAST_STATE_JSON), any(TypeReference.class)))
                    .thenReturn(Collections.emptyList());
            // Call 3: tryBuildFastOperatorStateMetaInfo parses serializer object
            jsonHelperMock.when(() -> JsonHelper.fromJson(eq("fast-serializer-obj"), eq(HashMap.class)))
                    .thenReturn(fastSerializerMap);
            // Call 4: getFastOperatorStateSerializer parses the inner serializer JSON
            jsonHelperMock.when(() -> JsonHelper.fromJson(eq("{\"type\":3}"), any(TypeReference.class)))
                    .thenReturn(fastSerializerTypeMap);

            wrapper.writeOperatorMetaData(provider,
                    nonEmptyOpJson, EMPTY_BROADCAST_STATE_JSON);

            verify(omniTask).writeOperatorMetaDataBytes(eq(provider), any(byte[].class));
        }
    }

    // ========== materializeMetaData tests ==========

    private static final String MMD_STATE_JSON = "[{\"name\":\"mmdState\",\"backendStateType\":1,"
            + "\"options\":{\"keyedStateType\":\"VALUE\"},"
            + "\"serializer\":\"{}\"}]";

    @Test
    @DisplayName("full path: JSON parsed -> StateMetaInfoSnapshot built -> omniTask.materializeMetaData returns result")
    void testMaterializeMetaDataHappyPath() throws Exception {
        when(omniTask.getExecutionConfig()).thenReturn(executionConfig);
        when(omniTask.getCheckpointingEnv()).thenReturn(runtimeEnvironment);
        when(runtimeEnvironment.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        when(userCodeClassLoader.asClassLoader()).thenReturn(getClass().getClassLoader());
        when(omniTask.materializeMetaData(eq(CHECKPOINT_ID), anyList(), any(), any(), any()))
                .thenReturn(snapshotResult);

        OmniStateMetaSerializerInfo.Builder mockBuilder = mock(OmniStateMetaSerializerInfo.Builder.class);
        OmniStateMetaSerializerInfo mockSerializerInfo = mock(OmniStateMetaSerializerInfo.class);
        when(mockBuilder.build()).thenReturn(mockSerializerInfo);
        when(mockSerializerInfo.getSerializerSnapshotGroup()).thenReturn(Collections.emptyMap());
        when(mockSerializerInfo.getSerializerGroup()).thenReturn(Collections.emptyMap());

        try (MockedStatic<OmniStateSerializerHelper> helperMock =
                     mockStatic(OmniStateSerializerHelper.class);
             MockedStatic<JsonHelper> jsonHelperMock = mockStatic(JsonHelper.class)) {

            jsonHelperMock.when(() -> JsonHelper.fromJson(anyString(), eq(HashMap.class)))
                    .thenReturn(new HashMap<>());

            helperMock.when(() -> OmniStateSerializerHelper.getStateBackendKeySerializer(
                            anyMap(), eq(executionConfig), any()))
                    .thenReturn((TypeSerializer) StringSerializer.INSTANCE);

            helperMock.when(() -> OmniStateSerializerHelper.buildSerializerInfo(
                            eq("mmdState"), eq(1), anyMap(), eq(executionConfig), any()))
                    .thenReturn(mockBuilder);

            SnapshotResult<StreamStateHandle> result =
                    wrapper.materializeMetaData(CHECKPOINT_ID, MMD_STATE_JSON, "{}", VALID_CHECKPOINT_OPTION_JSON);

            assertSame(snapshotResult, result);
            verify(omniTask).materializeMetaData(eq(CHECKPOINT_ID), anyList(), any(), any(), any());
        }
    }

    @Test
    @DisplayName("internal exception wrapped in IOException")
    void testMaterializeMetaDataThrowsIOExceptionOnException() throws Exception {
        when(omniTask.getExecutionConfig()).thenReturn(executionConfig);
        when(omniTask.getCheckpointingEnv()).thenThrow(new RuntimeException("Env error"));

        Exception ex = assertThrows(IOException.class, () ->
                wrapper.materializeMetaData(CHECKPOINT_ID, MMD_STATE_JSON, "{}", VALID_CHECKPOINT_OPTION_JSON));

        assertTrue(ex.getMessage().contains("Failed to materialize metadata"));
    }

    @Test
    @DisplayName("invalid state JSON -> IOException")
    void testMaterializeMetaDataWithInvalidJsonThrowsIOException() {
        Exception ex = assertThrows(IOException.class, () ->
                wrapper.materializeMetaData(CHECKPOINT_ID, "invalid", "{}", VALID_CHECKPOINT_OPTION_JSON));

        assertTrue(ex.getMessage().contains("Failed to materialize metadata"));
    }

    private static final String LOCAL_RECOVERY_CONFIG_JSON =
            "{\"allocationBaseDirs\":[\"/tmp/localRecovery\"],"
            + "\"jobID\":\"00112233445566778899aabbccddeeff\","
            + "\"jobVertexID\":\"11111111111111111111111111111111\","
            + "\"subtaskIndex\":0}";

    @Test
    @DisplayName("localRecoveryConfigStr != {} -> builds LocalRecoveryConfig with directory provider")
    void testMaterializeMetaDataWithLocalRecoveryConfig() throws Exception {
        when(omniTask.getExecutionConfig()).thenReturn(executionConfig);
        when(omniTask.getCheckpointingEnv()).thenReturn(runtimeEnvironment);
        when(runtimeEnvironment.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        when(userCodeClassLoader.asClassLoader()).thenReturn(getClass().getClassLoader());
        when(omniTask.materializeMetaData(eq(CHECKPOINT_ID), anyList(), any(), any(), any()))
                .thenReturn(snapshotResult);

        OmniStateMetaSerializerInfo.Builder mockBuilder = mock(OmniStateMetaSerializerInfo.Builder.class);
        OmniStateMetaSerializerInfo mockSerializerInfo = mock(OmniStateMetaSerializerInfo.class);
        when(mockBuilder.build()).thenReturn(mockSerializerInfo);
        when(mockSerializerInfo.getSerializerSnapshotGroup()).thenReturn(Collections.emptyMap());
        when(mockSerializerInfo.getSerializerGroup()).thenReturn(Collections.emptyMap());

        try (MockedStatic<OmniStateSerializerHelper> helperMock =
                     mockStatic(OmniStateSerializerHelper.class);
             MockedStatic<JsonHelper> jsonHelperMock = mockStatic(JsonHelper.class)) {

            jsonHelperMock.when(() -> JsonHelper.fromJson(anyString(), eq(HashMap.class)))
                    .thenReturn(new HashMap<>());

            helperMock.when(() -> OmniStateSerializerHelper.getStateBackendKeySerializer(
                            anyMap(), eq(executionConfig), any()))
                    .thenReturn((TypeSerializer) StringSerializer.INSTANCE);

            helperMock.when(() -> OmniStateSerializerHelper.buildSerializerInfo(
                            anyString(), anyInt(), anyMap(), eq(executionConfig), any()))
                    .thenReturn(mockBuilder);

            SnapshotResult<StreamStateHandle> result =
                    wrapper.materializeMetaData(CHECKPOINT_ID, MMD_STATE_JSON,
                            LOCAL_RECOVERY_CONFIG_JSON, VALID_CHECKPOINT_OPTION_JSON);

            assertSame(snapshotResult, result);
            verify(omniTask).materializeMetaData(eq(CHECKPOINT_ID), anyList(), notNull(), any(), any());
        }
    }

    @Test
    @DisplayName("null builder from buildSerializerInfo -> serializerInfo stays null, emptyMap used for snapshots")
    void testMaterializeMetaDataWithNullBuilder() throws Exception {
        when(omniTask.getExecutionConfig()).thenReturn(executionConfig);
        when(omniTask.getCheckpointingEnv()).thenReturn(runtimeEnvironment);
        when(runtimeEnvironment.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        when(userCodeClassLoader.asClassLoader()).thenReturn(getClass().getClassLoader());
        when(omniTask.materializeMetaData(eq(CHECKPOINT_ID), anyList(), any(), any(), any()))
                .thenReturn(snapshotResult);

        try (MockedStatic<OmniStateSerializerHelper> helperMock =
                     mockStatic(OmniStateSerializerHelper.class);
             MockedStatic<JsonHelper> jsonHelperMock = mockStatic(JsonHelper.class)) {

            jsonHelperMock.when(() -> JsonHelper.fromJson(anyString(), eq(HashMap.class)))
                    .thenReturn(new HashMap<>());

            helperMock.when(() -> OmniStateSerializerHelper.getStateBackendKeySerializer(
                            anyMap(), eq(executionConfig), any()))
                    .thenReturn((TypeSerializer) StringSerializer.INSTANCE);

            // buildSerializerInfo returns null → serializerInfo stays null
            helperMock.when(() -> OmniStateSerializerHelper.buildSerializerInfo(
                            anyString(), anyInt(), anyMap(), eq(executionConfig), any()))
                    .thenReturn(null);

            SnapshotResult<StreamStateHandle> result =
                    wrapper.materializeMetaData(CHECKPOINT_ID, MMD_STATE_JSON, "{}", VALID_CHECKPOINT_OPTION_JSON);

            assertSame(snapshotResult, result);
        }
    }

    @Test
    @DisplayName("multiple states: second loop skips keySerializer build (already non-null)")
    void testMaterializeMetaDataWithMultipleStates() throws Exception {
        String multiStateJson = "["
                + "{\"name\":\"s1\",\"backendStateType\":1,\"options\":{\"keyedStateType\":\"VALUE\"},\"serializer\":\"{}\"},"
                + "{\"name\":\"s2\",\"backendStateType\":2,\"options\":{},\"serializer\":\"{}\"}"
                + "]";

        when(omniTask.getExecutionConfig()).thenReturn(executionConfig);
        when(omniTask.getCheckpointingEnv()).thenReturn(runtimeEnvironment);
        when(runtimeEnvironment.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        when(userCodeClassLoader.asClassLoader()).thenReturn(getClass().getClassLoader());
        when(omniTask.materializeMetaData(eq(CHECKPOINT_ID), anyList(), any(), any(), any()))
                .thenReturn(snapshotResult);

        OmniStateMetaSerializerInfo.Builder mockBuilder = mock(OmniStateMetaSerializerInfo.Builder.class);
        OmniStateMetaSerializerInfo mockSerializerInfo = mock(OmniStateMetaSerializerInfo.class);
        when(mockBuilder.build()).thenReturn(mockSerializerInfo);
        when(mockSerializerInfo.getSerializerSnapshotGroup()).thenReturn(Collections.emptyMap());
        when(mockSerializerInfo.getSerializerGroup()).thenReturn(Collections.emptyMap());

        try (MockedStatic<OmniStateSerializerHelper> helperMock =
                     mockStatic(OmniStateSerializerHelper.class);
             MockedStatic<JsonHelper> jsonHelperMock = mockStatic(JsonHelper.class)) {

            jsonHelperMock.when(() -> JsonHelper.fromJson(anyString(), eq(HashMap.class)))
                    .thenReturn(new HashMap<>());

            // Only called once (first state), second state skips because keySerializer already non-null
            helperMock.when(() -> OmniStateSerializerHelper.getStateBackendKeySerializer(
                            anyMap(), eq(executionConfig), any()))
                    .thenReturn((TypeSerializer) StringSerializer.INSTANCE);

            helperMock.when(() -> OmniStateSerializerHelper.buildSerializerInfo(
                            anyString(), anyInt(), anyMap(), eq(executionConfig), any()))
                    .thenReturn(mockBuilder);

            SnapshotResult<StreamStateHandle> result =
                    wrapper.materializeMetaData(CHECKPOINT_ID, multiStateJson, "{}", VALID_CHECKPOINT_OPTION_JSON);

            assertSame(snapshotResult, result);
            // getStateBackendKeySerializer called only once (first iteration)
            helperMock.verify(
                    () -> OmniStateSerializerHelper.getStateBackendKeySerializer(
                            anyMap(), eq(executionConfig), any()),
                    times(1));
            // buildSerializerInfo called for both states
            helperMock.verify(
                    () -> OmniStateSerializerHelper.buildSerializerInfo(
                            anyString(), anyInt(), anyMap(), eq(executionConfig), any()),
                    times(2));
        }
    }

    @Test
    @DisplayName("options without keyedStateType -> OmniSerializerKeyedStateType.get returns null, LOG.warn path")
    void testMaterializeMetaDataWithNullKeyedStateType() throws Exception {
        String stateJson = "[{\"name\":\"unknownState\",\"backendStateType\":1,"
                + "\"options\":{},\"serializer\":\"{}\"}]";

        when(omniTask.getExecutionConfig()).thenReturn(executionConfig);
        when(omniTask.getCheckpointingEnv()).thenReturn(runtimeEnvironment);
        when(runtimeEnvironment.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
        when(userCodeClassLoader.asClassLoader()).thenReturn(getClass().getClassLoader());
        when(omniTask.materializeMetaData(eq(CHECKPOINT_ID), anyList(), any(), any(), any()))
                .thenReturn(snapshotResult);

        OmniStateMetaSerializerInfo.Builder mockBuilder = mock(OmniStateMetaSerializerInfo.Builder.class);
        OmniStateMetaSerializerInfo mockSerializerInfo = mock(OmniStateMetaSerializerInfo.class);
        when(mockBuilder.build()).thenReturn(mockSerializerInfo);
        when(mockSerializerInfo.getSerializerSnapshotGroup()).thenReturn(Collections.emptyMap());
        when(mockSerializerInfo.getSerializerGroup()).thenReturn(Collections.emptyMap());

        try (MockedStatic<OmniStateSerializerHelper> helperMock =
                     mockStatic(OmniStateSerializerHelper.class);
             MockedStatic<JsonHelper> jsonHelperMock = mockStatic(JsonHelper.class)) {

            jsonHelperMock.when(() -> JsonHelper.fromJson(anyString(), eq(HashMap.class)))
                    .thenReturn(new HashMap<>());

            helperMock.when(() -> OmniStateSerializerHelper.getStateBackendKeySerializer(
                            anyMap(), eq(executionConfig), any()))
                    .thenReturn((TypeSerializer) StringSerializer.INSTANCE);

            helperMock.when(() -> OmniStateSerializerHelper.buildSerializerInfo(
                            anyString(), anyInt(), anyMap(), eq(executionConfig), any()))
                    .thenReturn(mockBuilder);

            SnapshotResult<StreamStateHandle> result =
                    wrapper.materializeMetaData(CHECKPOINT_ID, stateJson, "{}", VALID_CHECKPOINT_OPTION_JSON);

            assertSame(snapshotResult, result);
        }
    }

    // ========== declineCheckpoint tests (private, via reflection) ==========

    @Test
    @DisplayName("CHECKPOINT_DECLINED reason with full exception string -> delegated via reflection")
    void testDeclineCheckpointDelegatesToOmniTask() throws Exception {
        Method method = OmniTaskWrapper.class.getDeclaredMethod(
                "declineCheckpoint", String.class, String.class, String.class);
        method.setAccessible(true);

        String failureReason = "CHECKPOINT_DECLINED";
        String exceptionStr = "Error Code:42\nReason:test-reason\nStack:test-stack";
        method.invoke(wrapper, "100", failureReason, exceptionStr);

        verify(omniTask).declineCheckpoint(eq(100L), eq(CheckpointFailureReason.CHECKPOINT_DECLINED), any(Throwable.class));
    }

    @Test
    @DisplayName("CHECKPOINT_DECLINED_SUBSUMED + nullptr exception -> null throwable")
    void testDeclineCheckpointWithSubsumedReason() throws Exception {
        Method method = OmniTaskWrapper.class.getDeclaredMethod(
                "declineCheckpoint", String.class, String.class, String.class);
        method.setAccessible(true);

        method.invoke(wrapper, "200", "CHECKPOINT_DECLINED_SUBSUMED", "nullptr");

        verify(omniTask).declineCheckpoint(eq(200L), eq(CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED), isNull());
    }

    @Test
    @DisplayName("unknown failure reason -> UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE")
    void testDeclineCheckpointWithUnknownReason() throws Exception {
        Method method = OmniTaskWrapper.class.getDeclaredMethod(
                "declineCheckpoint", String.class, String.class, String.class);
        method.setAccessible(true);

        method.invoke(wrapper, "300", "UNKNOWN_FOO", "nullptr");

        verify(omniTask).declineCheckpoint(eq(300L), eq(CheckpointFailureReason.UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE), isNull());
    }

    @Test
    @DisplayName("CHECKPOINT_DECLINED_TASK_NOT_READY with exception string")
    void testDeclineCheckpointWithTaskNotReady() throws Exception {
        Method method = OmniTaskWrapper.class.getDeclaredMethod(
                "declineCheckpoint", String.class, String.class, String.class);
        method.setAccessible(true);

        String exceptionStr = "Error Code:5\nReason:not-ready\nStack:null";
        method.invoke(wrapper, "42", "CHECKPOINT_DECLINED_TASK_NOT_READY", exceptionStr);

        verify(omniTask).declineCheckpoint(eq(42L), eq(CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY), any(Throwable.class));
    }
}
