/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
package org.apache.flink.runtime.state.filesystem;

import com.huawei.omniruntime.flink.core.fs.ByteBufferWritable;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FsCheckpointStateOutputStream 单元测试，
 * 重点覆盖 write(ByteBuffer) 四路分支和 closeAndGetHandle 的 inline threshold 逻辑。
 */
public class FsCheckpointStateOutputStreamByteBufferTest {

    private File tempDir;
    private FileSystem fs;
    private Path basePath;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("fsckpt_").toFile();
        basePath = new Path(tempDir.toURI().toString());
        fs = new LocalFileSystem();
    }

    @AfterEach
    void tearDown() {
        if (tempDir.exists()) {
            File[] files = tempDir.listFiles();
            if (files != null) {
                for (File f : files) {
                    f.delete();
                }
            }
            tempDir.delete();
        }
    }

    /**
     * write(int) 写入单字节，pos 应为 1。
     */
    @Test
    void testWriteSingleByte() throws IOException {
        FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        basePath, fs, 4096, 1024);

        stream.write(0x42);
        assertEquals(1, stream.getPos());
        stream.close();
    }

    /**
     * write(byte[]) 写入小于 buffer 的数组，数据暂留在 writeBuffer 中。
     */
    @Test
    void testWriteByteArraySmallerThanBuffer() throws IOException {
        FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        basePath, fs, 4096, 1024);

        byte[] data = new byte[100];
        for (int i = 0; i < 100; i++) {
            data[i] = (byte) i;
        }
        stream.write(data);
        assertEquals(100, stream.getPos());
        stream.close();
    }

    /**
     * write(ByteBuffer) 空 buffer 直接返回 true，pos 不变。
     */
    @Test
    void testWriteByteBufferEmpty() throws IOException {
        FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        basePath, fs, 4096, 1024);

        ByteBuffer buffer = ByteBuffer.allocate(0);
        boolean result = stream.write(buffer);

        assertTrue(result);
        assertEquals(0, stream.getPos());
        stream.close();
    }

    /**
     * hasArray 的 ByteBuffer 走内部 write(byte[], off, len) 路径，返回 false。
     */
    @Test
    void testWriteByteBufferHasArray() throws IOException {
        FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        basePath, fs, 4096, 1024);

        byte[] data = new byte[] {1, 2, 3};
        ByteBuffer buffer = ByteBuffer.wrap(data);
        boolean result = stream.write(buffer);

        assertFalse(result);
        assertFalse(buffer.hasRemaining());
        assertEquals(3, stream.getPos());
        stream.close();
    }

    /**
     * 小于 writeBuffer 的 DirectByteBuffer 复制到内部 writeBuffer，返回 false。
     */
    @Test
    void testWriteByteBufferSmallDirect() throws IOException {
        FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        basePath, fs, 4096, 1024);

        ByteBuffer buffer = ByteBuffer.allocateDirect(100);
        for (int i = 0; i < 100; i++) {
            buffer.put((byte) i);
        }
        buffer.flip();

        boolean result = stream.write(buffer);

        assertFalse(result);
        assertFalse(buffer.hasRemaining());
        assertEquals(100, stream.getPos());
        stream.close();
    }

    /**
     * 大于 writeBuffer 的 DirectByteBuffer 触发 flushToFile 后走 delegate 的
     * ByteBufferWritable 快速路径（LocalFileSystem 的 LocalDataOutputStream 支持零拷贝），返回 true。
     */
    @Test
    void testWriteByteBufferLargeDirectNativePath() throws IOException {
        FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        basePath, fs, 4096, 1024);

        ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
        byte fill = 0x5A;
        for (int i = 0; i < 8192; i++) {
            buffer.put(fill);
        }
        buffer.flip();

        boolean result = stream.write(buffer);

        assertTrue(result);
        assertFalse(buffer.hasRemaining());
        assertEquals(8192, stream.getPos());
        stream.close();
    }

    /**
     * 写入量小于 localStateThreshold 时 closeAndGetHandle 返回 ByteStreamStateHandle。
     */
    @Test
    void testCloseAndGetHandleInlineThreshold() throws IOException {
        FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        basePath, fs, 4096, 128);

        stream.write(new byte[] {1, 2, 3});
        StreamStateHandle handle = stream.closeAndGetHandle();

        assertNotNull(handle);
        assertTrue(handle instanceof ByteStreamStateHandle);
        ByteStreamStateHandle byteHandle = (ByteStreamStateHandle) handle;
        assertEquals(3, byteHandle.getData().length);
        assertEquals(1, byteHandle.getData()[0]);
        assertEquals(3, byteHandle.getData()[2]);
    }

    /**
     * 写入量超过 localStateThreshold 时 closeAndGetHandle 返回 FileStateHandle 或 RelativeFileStateHandle。
     */
    @Test
    void testCloseAndGetHandleExceedsThreshold() throws IOException {
        FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        basePath, fs, 4096, 10);

        byte[] data = new byte[100];
        for (int i = 0; i < 100; i++) {
            data[i] = (byte) i;
        }
        stream.write(data);
        StreamStateHandle handle = stream.closeAndGetHandle();

        assertNotNull(handle);
        assertTrue(handle instanceof FileStateHandle || handle instanceof RelativeFileStateHandle);
    }

    /**
     * 无任何写入时 closeAndGetHandle 返回 null。
     */
    @Test
    void testCloseAndGetHandleNoDataReturnsNull() throws IOException {
        FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        basePath, fs, 4096, 1024);

        StreamStateHandle handle = stream.closeAndGetHandle();
        assertNull(handle);
    }

    /**
     * FsCheckpointStateOutputStream 实现了 ByteBufferWritable 接口。
     */
    @Test
    void testImplementsByteBufferWritable() {
        FsCheckpointStreamFactory.FsCheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        basePath, fs, 4096, 1024);

        assertTrue(stream instanceof ByteBufferWritable);
    }
}
