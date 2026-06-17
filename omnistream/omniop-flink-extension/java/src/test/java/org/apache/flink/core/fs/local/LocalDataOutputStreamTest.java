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
package org.apache.flink.core.fs.local;

import com.huawei.omniruntime.flink.core.fs.ByteBufferWritable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LocalDataOutputStream 单元测试，覆盖 ByteBufferWritable 零拷贝写入路径及关闭后异常检查。
 */
public class LocalDataOutputStreamTest {

    private File tempFile;
    private LocalDataOutputStream stream;

    @BeforeEach
    void setUp() throws IOException {
        tempFile = File.createTempFile("ldos_test_", ".tmp");
        stream = new LocalDataOutputStream(tempFile);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (stream != null) {
            try {
                stream.close();
            } catch (IOException ignored) {
            }
        }
        Files.deleteIfExists(tempFile.toPath());
    }

    /**
     * write(int) 写入单字节，应正确持久化到文件中。
     */
    @Test
    void testWriteInt() throws IOException {
        stream.write(0x42);
        stream.close();

        byte[] content = Files.readAllBytes(tempFile.toPath());
        assertEquals(1, content.length);
        assertEquals((byte) 0x42, content[0]);
    }

    /**
     * write(byte[]) 写入字节数组，文件内容应与输入一致。
     */
    @Test
    void testWriteByteArray() throws IOException {
        byte[] data = {1, 2, 3, 4, 5};
        stream.write(data);
        stream.close();

        byte[] content = Files.readAllBytes(tempFile.toPath());
        assertEquals(5, content.length);
        assertEquals(data[0], content[0]);
        assertEquals(data[4], content[4]);
    }

    /**
     * write(byte[], off, len) 从指定偏移写入指定长度。
     */
    @Test
    void testWriteByteArrayWithOffset() throws IOException {
        byte[] data = {0, 0, 10, 20, 30};
        stream.write(data, 2, 3);
        stream.close();

        byte[] content = Files.readAllBytes(tempFile.toPath());
        assertEquals(3, content.length);
        assertEquals((byte) 10, content[0]);
        assertEquals((byte) 30, content[2]);
    }

    /**
     * DirectByteBuffer 写入走 FileChannel 零拷贝路径，write 返回 true，buffer 被完全消费。
     */
    @Test
    void testWriteByteBufferDirect() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(4);
        buffer.putInt(0x01020304);
        buffer.flip();

        assertTrue(stream instanceof ByteBufferWritable);
        boolean nativePath = ((ByteBufferWritable) stream).write(buffer);
        assertTrue(nativePath);
        assertFalse(buffer.hasRemaining());
        stream.close();

        byte[] content = Files.readAllBytes(tempFile.toPath());
        assertEquals(4, content.length);
    }

    /**
     * Heap ByteBuffer 同样走 FileChannel 零拷贝路径，write 返回 true。
     */
    @Test
    void testWriteByteBufferHeapArray() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {0x0A, 0x0B, 0x0C});
        boolean nativePath = ((ByteBufferWritable) stream).write(buffer);
        assertTrue(nativePath);
        stream.close();

        byte[] content = Files.readAllBytes(tempFile.toPath());
        assertEquals(3, content.length);
        assertEquals((byte) 0x0A, content[0]);
    }

    /**
     * flush 和 sync 调用不抛异常，数据最终写入文件。
     */
    @Test
    void testFlushAndSync() throws IOException {
        stream.write(0xAA);
        stream.flush();
        stream.sync();
        stream.close();

        byte[] content = Files.readAllBytes(tempFile.toPath());
        assertEquals(1, content.length);
    }

    /**
     * getPos 返回当前文件写入位置，随每次写入递增。
     */
    @Test
    void testGetPos() throws IOException {
        stream.write(new byte[] {1, 2, 3});
        assertEquals(3, stream.getPos());

        stream.write(new byte[] {4, 5});
        assertEquals(5, stream.getPos());
        stream.close();
    }

    /**
     * 已关闭 stream 上调用 write(int)、write(byte[])、write(byte[], off, len) 均应抛 ClosedChannelException。
     */
    @Test
    void testClosedThrowsWrite() throws IOException {
        stream.close();

        assertThrows(ClosedChannelException.class, () -> stream.write(0x42));
        assertThrows(ClosedChannelException.class, () -> stream.write(new byte[] {1}));
        assertThrows(ClosedChannelException.class, () -> stream.write(new byte[] {1}, 0, 1));
    }

    /**
     * 已关闭 stream 上调用 write(ByteBuffer) 同样应抛 ClosedChannelException。
     */
    @Test
    void testClosedThrowsWriteByteBuffer() throws IOException {
        stream.close();

        ByteBuffer buffer = ByteBuffer.allocate(1);
        assertThrows(ClosedChannelException.class, () -> ((ByteBufferWritable) stream).write(buffer));
    }
}
