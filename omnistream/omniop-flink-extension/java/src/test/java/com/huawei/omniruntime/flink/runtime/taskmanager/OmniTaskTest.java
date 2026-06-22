/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.taskmanager;

import com.huawei.omniruntime.flink.core.fs.ByteBufferWritable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link OmniTask} savepoint output stream operations.
 */
public class OmniTaskTest {

    private OmniTask omniTask;

    private CheckpointStreamWithResultProvider provider;
    private CheckpointStateOutputStream outputStream;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        provider = mock(CheckpointStreamWithResultProvider.class);
        outputStream = mock(CheckpointStateOutputStream.class);
        when(provider.getCheckpointOutputStream()).thenReturn(outputStream);

        omniTask = mock(OmniTask.class, CALLS_REAL_METHODS);
    }

    // ========== closeSavepointOutputStream tests ==========

    @Test
    @DisplayName("closeAndFinalizeCheckpointStreamResult returns valid handle -> SnapshotResult.of")
    void testCloseSavepointOutputStreamReturnsSnapshot() throws Exception {
        StreamStateHandle handle = mock(StreamStateHandle.class);
        when(provider.closeAndFinalizeCheckpointStreamResult())
                .thenReturn(SnapshotResult.of(handle));

        SnapshotResult<StreamStateHandle> result = omniTask.closeSavepointOutputStream(provider);

        assertNotNull(result);
        assertSame(handle, result.getJobManagerOwnedSnapshot());
    }

    @Test
    @DisplayName("provider.closeAndFinalizeCheckpointStreamResult throws -> propagated")
    void testCloseSavepointOutputStreamPropagatesException() throws Exception {
        when(provider.closeAndFinalizeCheckpointStreamResult())
                .thenThrow(new IOException("Close failed"));

        Exception ex = assertThrows(IOException.class, () ->
                omniTask.closeSavepointOutputStream(provider));

        assertTrue(ex.getMessage().contains("Close failed"));
    }

    // ========== writeSavepointOutputStream tests ==========

    @Test
    @DisplayName("writes byte[] chunk to CheckpointStateOutputStream")
    void testWriteSavepointOutputStreamWritesChunk() throws Exception {
        byte[] chunk = new byte[]{1, 2, 3, 4, 5};
        doNothing().when(outputStream).write(chunk);

        omniTask.writeSavepointOutputStream(provider, chunk);

        verify(outputStream).write(chunk);
    }

    @Test
    @DisplayName("writes empty byte[] chunk")
    void testWriteSavepointOutputStreamWritesEmptyChunk() throws Exception {
        byte[] emptyChunk = new byte[0];
        doNothing().when(outputStream).write(emptyChunk);

        omniTask.writeSavepointOutputStream(provider, emptyChunk);

        verify(outputStream).write(emptyChunk);
    }

    @Test
    @DisplayName("outputStream.write throws -> wrapped in IOException")
    void testWriteSavepointOutputStreamWrapsException() throws Exception {
        byte[] chunk = new byte[]{1, 2, 3};
        doThrow(new RuntimeException("Write fail")).when(outputStream).write(chunk);

        Exception ex = assertThrows(IOException.class, () ->
                omniTask.writeSavepointOutputStream(provider, chunk));

        assertTrue(ex.getMessage().contains("Failed to writeSavepointOutputStream"));
    }

    // ========== writeSavepointOutputStreamDirect tests ==========

    @Test
    @DisplayName("null provider -> IOException")
    void testWriteSavepointOutputStreamDirectNullProvider() {
        ByteBuffer chunk = ByteBuffer.allocate(10);
        Exception ex = assertThrows(IOException.class, () ->
                omniTask.writeSavepointOutputStreamDirect(null, chunk, 10));

        assertTrue(ex.getMessage().contains("Failed to writeSavepointOutputStreamDirect"));
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("null"));
    }

    @Test
    @DisplayName("null chunk -> IOException")
    void testWriteSavepointOutputStreamDirectNullChunk() {
        Exception ex = assertThrows(IOException.class, () ->
                omniTask.writeSavepointOutputStreamDirect(provider, null, 10));

        assertTrue(ex.getMessage().contains("Failed to writeSavepointOutputStreamDirect"));
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("null"));
    }

    @Test
    @DisplayName("negative len -> IOException")
    void testWriteSavepointOutputStreamDirectNegativeLength() {
        ByteBuffer chunk = ByteBuffer.allocate(10);
        Exception ex = assertThrows(IOException.class, () ->
                omniTask.writeSavepointOutputStreamDirect(provider, chunk, -1));

        assertTrue(ex.getMessage().contains("Failed to writeSavepointOutputStreamDirect"));
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("Invalid"));
    }

    @Test
    @DisplayName("len exceeds capacity -> IOException")
    void testWriteSavepointOutputStreamDirectLengthExceedsCapacity() {
        ByteBuffer chunk = ByteBuffer.allocate(10);
        Exception ex = assertThrows(IOException.class, () ->
                omniTask.writeSavepointOutputStreamDirect(provider, chunk, 20));

        assertTrue(ex.getMessage().contains("Failed to writeSavepointOutputStreamDirect"));
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("Invalid"));
    }

    @Test
    @DisplayName("direct ByteBuffer + ByteBufferWritable outputStream -> zero-copy write returns true")
    void testWriteSavepointOutputStreamDirectZeroCopyPath() throws Exception {
        ByteBuffer chunk = ByteBuffer.allocateDirect(10);
        chunk.put(new byte[]{1, 2, 3, 4, 5});

        // Create a mock that implements ByteBufferWritable
        ByteBufferWritableOutputStream mockWritableStream = mock(ByteBufferWritableOutputStream.class);
        when(mockWritableStream.write(any(ByteBuffer.class))).thenReturn(true);
        when(provider.getCheckpointOutputStream()).thenReturn(mockWritableStream);

        boolean result = omniTask.writeSavepointOutputStreamDirect(provider, chunk, 5);

        assertTrue(result);
        verify(mockWritableStream).write(any(ByteBuffer.class));
    }

    @Test
    @DisplayName("direct ByteBuffer + ByteBufferWritable outputStream returns false -> propagated")
    void testWriteSavepointOutputStreamDirectZeroCopyReturnsFalse() throws Exception {
        ByteBuffer chunk = ByteBuffer.allocateDirect(10);
        chunk.put(new byte[]{1, 2, 3});

        ByteBufferWritableOutputStream mockWritableStream = mock(ByteBufferWritableOutputStream.class);
        when(mockWritableStream.write(any(ByteBuffer.class))).thenReturn(false);
        when(provider.getCheckpointOutputStream()).thenReturn(mockWritableStream);

        boolean result = omniTask.writeSavepointOutputStreamDirect(provider, chunk, 3);

        assertFalse(result);
    }

    @Test
    @DisplayName("non-direct ByteBuffer -> heap-copy fallback write")
    void testWriteSavepointOutputStreamDirectHeapFallback() throws Exception {
        setupFallbackBuffer();
        ByteBuffer chunk = ByteBuffer.allocate(10); // non-direct
        chunk.put(new byte[]{1, 2, 3});

        byte[] expectedBytes = new byte[]{1, 2, 3};
        doNothing().when(outputStream).write(any(byte[].class), eq(0), eq(3));

        boolean result = omniTask.writeSavepointOutputStreamDirect(provider, chunk, 3);

        assertFalse(result);
        verify(outputStream).write(any(byte[].class), eq(0), eq(3));
    }

    @Test
    @DisplayName("outputStream.write throws in direct path -> IOException")
    void testWriteSavepointOutputStreamDirectWrapsException() throws Exception {
        ByteBuffer chunk = ByteBuffer.allocateDirect(10);
        chunk.put(new byte[]{1, 2, 3});

        ByteBufferWritableOutputStream mockWritableStream = mock(ByteBufferWritableOutputStream.class);
        when(mockWritableStream.write(any(ByteBuffer.class))).thenThrow(new RuntimeException("Write error"));
        when(provider.getCheckpointOutputStream()).thenReturn(mockWritableStream);

        Exception ex = assertThrows(IOException.class, () ->
                omniTask.writeSavepointOutputStreamDirect(provider, chunk, 3));

        assertTrue(ex.getMessage().contains("Failed to writeSavepointOutputStreamDirect"));
    }

    // ========== getSavepointOutputStreamPos tests ==========

    @Test
    @DisplayName("delegates to CheckpointStateOutputStream.getPos()")
    void testGetSavepointOutputStreamPosReturnsPosition() throws Exception {
        when(outputStream.getPos()).thenReturn(1024L);

        long pos = omniTask.getSavepointOutputStreamPos(provider);

        assertEquals(1024L, pos);
        verify(outputStream).getPos();
    }

    @Test
    @DisplayName("getCheckpointOutputStream throws -> propagated")
    void testGetSavepointOutputStreamPosPropagatesException() throws Exception {
        when(outputStream.getPos()).thenThrow(new IOException("Pos error"));

        Exception ex = assertThrows(IOException.class, () ->
                omniTask.getSavepointOutputStreamPos(provider));

        assertTrue(ex.getMessage().contains("Pos error"));
    }

    // ========== uploadFilesToCheckpointFs tests ==========

    @Test
    @DisplayName("checkpointStreamFactory == null -> IllegalStateException")
    void testUploadFilesToCheckpointFsFactoryNotInitialized() {
        List<java.nio.file.Path> paths = Collections.emptyList();

        Exception ex = assertThrows(IllegalStateException.class, () ->
                omniTask.uploadFilesToCheckpointFs(paths, 1));

        assertTrue(ex.getMessage().contains("CheckpointStreamFactory is not initialized"));
    }

    @Test
    @DisplayName("null paths -> returns empty list")
    void testUploadFilesToCheckpointFsNullPaths() throws Exception {
        setCheckpointStreamFactory();

        List<HandleAndLocalPath> result = omniTask.uploadFilesToCheckpointFs(null, 1);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("empty paths -> returns empty list")
    void testUploadFilesToCheckpointFsEmptyPaths() throws Exception {
        setCheckpointStreamFactory();

        List<HandleAndLocalPath> result =
                omniTask.uploadFilesToCheckpointFs(Collections.emptyList(), 1);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    // ========== writeSavepointMetadata tests ==========

    @Test
    @DisplayName("non-null keySerializer used directly, KeyedBackendSerializationProxy writes to output stream")
    void testWriteSavepointMetadataWithNonNullKeySerializer() throws Exception {
        TypeSerializer<?> keySerializer = StringSerializer.INSTANCE;
        List<StateMetaInfoSnapshot> snapshots = Collections.emptyList();
        doNothing().when(outputStream).write(any(byte[].class), anyInt(), anyInt());

        omniTask.writeSavepointMetadata(provider, snapshots, keySerializer);

        // KeyedBackendSerializationProxy.write() writes to the stream
        verify(outputStream, atLeastOnce()).write(any(byte[].class), anyInt(), anyInt());
    }

    // ========== acquireSavepointOutputStream tests ==========

    @Test
    @DisplayName("acquires savepoint output stream via checkpointStorageAccess and returns the provider")
    void testAcquireSavepointOutputStream() throws Exception {
        CheckpointOptions options = CheckpointOptions.forCheckpointWithDefaultLocation();
        CheckpointStorageAccess checkpointAccess = mock(CheckpointStorageAccess.class);
        CheckpointStreamFactory streamFactory = mock(CheckpointStreamFactory.class);
        when(checkpointAccess.resolveCheckpointStorageLocation(anyLong(), any()))
                .thenReturn(streamFactory);

        @SuppressWarnings("unchecked")
        StreamTask<?, ?> streamTask = mock(StreamTask.class);
        org.apache.flink.runtime.execution.Environment env =
                mock(org.apache.flink.runtime.execution.Environment.class);
        when(streamTask.getEnvironment()).thenReturn(env);
        when(env.getCheckpointStorageAccess()).thenReturn(checkpointAccess);

        // inject invokable into the mock via reflection (OmniTask extends Task)
        Field invokableField = omniTask.getClass().getSuperclass().getDeclaredField("invokable");
        invokableField.setAccessible(true);
        invokableField.set(omniTask, streamTask);

        CheckpointStreamWithResultProvider expectedProvider =
                mock(CheckpointStreamWithResultProvider.class);
        try (MockedStatic<CheckpointStreamWithResultProvider> providerStatic =
                     mockStatic(CheckpointStreamWithResultProvider.class)) {
            providerStatic.when(() -> CheckpointStreamWithResultProvider.createSimpleStream(
                            any(), any()))
                    .thenReturn(expectedProvider);

            CheckpointStreamWithResultProvider result =
                    omniTask.acquireSavepointOutputStream(42L, options);

            assertSame(expectedProvider, result);
        } finally {
            invokableField.set(omniTask, null);
        }
    }

    // ========== helpers ==========

    /**
     * Sets checkpointStreamFactory on the mock via reflection to bypass null check.
     */
    private void setCheckpointStreamFactory() throws Exception {
        CheckpointStreamFactory factory = mock(CheckpointStreamFactory.class);
        Field factoryField = OmniTask.class.getDeclaredField("checkpointStreamFactory");
        factoryField.setAccessible(true);
        factoryField.set(omniTask, factory);
    }

    /**
     * Uses reflection to inject ThreadLocal into the mock so that the fallback
     * buffer path in writeSavepointOutputStreamDirect works correctly.
     */
    private void setupFallbackBuffer() throws Exception {
        Field field = OmniTask.class.getDeclaredField("savepointDirectFallbackBuffer");
        field.setAccessible(true);
        field.set(omniTask, ThreadLocal.withInitial(() -> new byte[1024 * 1024]));
    }

    // ========== inner classes ==========

    /**
     * A mock-friendly combination interface for testing the zero-copy path.
     */
    private abstract static class ByteBufferWritableOutputStream
            extends CheckpointStateOutputStream
            implements ByteBufferWritable {
    }
}
