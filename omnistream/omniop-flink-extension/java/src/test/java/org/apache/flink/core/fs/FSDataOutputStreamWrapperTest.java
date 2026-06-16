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
package org.apache.flink.core.fs;

import com.huawei.omniruntime.flink.core.fs.ByteBufferWritable;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FSDataOutputStreamWrapper 单元测试，覆盖委托转发及 ByteBufferWritable 三层回退路径。
 */
public class FSDataOutputStreamWrapperTest {

    /**
     * write(int) 委托到被包装的 stream。
     */
    @Test
    void testWriteInt() throws IOException {
        byte[] storage = new byte[1];
        TestFSDataOutputStream delegate = new TestFSDataOutputStream(storage);
        FSDataOutputStreamWrapper wrapper = new FSDataOutputStreamWrapper(delegate);

        wrapper.write(0x42);
        assertEquals((byte) 0x42, storage[0]);
    }

    /**
     * write(byte[]) 委托到被包装的 stream。
     */
    @Test
    void testWriteByteArray() throws IOException {
        byte[] storage = new byte[3];
        TestFSDataOutputStream delegate = new TestFSDataOutputStream(storage);
        FSDataOutputStreamWrapper wrapper = new FSDataOutputStreamWrapper(delegate);

        byte[] data = {1, 2, 3};
        wrapper.write(data);
        assertArrayEquals(data, storage);
    }

    /**
     * write(byte[], off, len) 委托到被包装的 stream。
     */
    @Test
    void testWriteByteArrayOffset() throws IOException {
        byte[] storage = new byte[2];
        TestFSDataOutputStream delegate = new TestFSDataOutputStream(storage);
        FSDataOutputStreamWrapper wrapper = new FSDataOutputStreamWrapper(delegate);

        byte[] data = {0, 10, 20, 0};
        wrapper.write(data, 1, 2);
        assertArrayEquals(new byte[] {10, 20}, storage);
    }

    /**
     * delegate 实现了 ByteBufferWritable 时，wrapper 直接转发，返回 delegate 的结果。
     */
    @Test
    void testWriteByteBufferNativePath() throws IOException {
        ByteBufferWritableDelegate delegate = new ByteBufferWritableDelegate();
        FSDataOutputStreamWrapper wrapper = new FSDataOutputStreamWrapper(delegate);

        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
        boolean result = wrapper.write(buffer);

        assertTrue(result);
        assertFalse(buffer.hasRemaining());
    }

    /**
     * delegate 未实现 ByteBufferWritable 且 buffer 有 array 时，回退到 write(byte[], off, len)。
     */
    @Test
    void testWriteByteBufferArrayFallback() throws IOException {
        byte[] storage = new byte[3];
        TestFSDataOutputStream delegate = new TestFSDataOutputStream(storage);
        FSDataOutputStreamWrapper wrapper = new FSDataOutputStreamWrapper(delegate);

        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {7, 8, 9});
        boolean result = wrapper.write(buffer);

        assertFalse(result);
        assertFalse(buffer.hasRemaining());
        assertArrayEquals(new byte[] {7, 8, 9}, storage);
    }

    /**
     * delegate 未实现 ByteBufferWritable 且 buffer 为 direct buffer 时，回退到 fallback copy buffer。
     */
    @Test
    void testWriteByteBufferDirectFallback() throws IOException {
        byte[] storage = new byte[4];
        TestFSDataOutputStream delegate = new TestFSDataOutputStream(storage);
        FSDataOutputStreamWrapper wrapper = new FSDataOutputStreamWrapper(delegate);

        ByteBuffer buffer = ByteBuffer.allocateDirect(4);
        buffer.putInt(0x01020304);
        buffer.flip();

        boolean result = wrapper.write(buffer);

        assertFalse(result);
        assertFalse(buffer.hasRemaining());
        assertEquals((byte) 0x01, storage[0]);
        assertEquals((byte) 0x02, storage[1]);
        assertEquals((byte) 0x03, storage[2]);
        assertEquals((byte) 0x04, storage[3]);
    }

    /**
     * getPos() 委托到被包装的 stream。
     */
    @Test
    void testGetPos() throws IOException {
        byte[] storage = new byte[10];
        TestFSDataOutputStream delegate = new TestFSDataOutputStream(storage);
        FSDataOutputStreamWrapper wrapper = new FSDataOutputStreamWrapper(delegate);

        wrapper.write(new byte[] {1, 2, 3});
        assertEquals(3, wrapper.getPos());
    }

    /**
     * getWrappedDelegate 返回原 delegate 实例。
     */
    @Test
    void testGetWrappedDelegate() {
        byte[] storage = new byte[1];
        TestFSDataOutputStream delegate = new TestFSDataOutputStream(storage);
        FSDataOutputStreamWrapper wrapper = new FSDataOutputStreamWrapper(delegate);

        assertSame(delegate, wrapper.getWrappedDelegate());
    }

    // ---- 测试替身 ----

    /** 将写操作捕获到 byte[] 的 FSDataOutputStream 最小实现。 */
    static class TestFSDataOutputStream extends FSDataOutputStream {
        private final byte[] storage;
        private int pos = 0;

        TestFSDataOutputStream(byte[] storage) {
            this.storage = storage;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void write(int b) {
            storage[pos++] = (byte) b;
        }

        @Override
        public void write(byte[] b) throws IOException {
            System.arraycopy(b, 0, storage, pos, b.length);
            pos += b.length;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            System.arraycopy(b, off, storage, pos, len);
            pos += len;
        }

        @Override
        public void flush() {}

        @Override
        public void sync() throws IOException {}

        @Override
        public void close() throws IOException {}
    }

    /** 实现了 ByteBufferWritable 的 FSDataOutputStream，write 返回 true。 */
    static class ByteBufferWritableDelegate extends FSDataOutputStream implements ByteBufferWritable {
        private int pos = 0;

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public boolean write(ByteBuffer buffer) {
            int len = buffer.remaining();
            buffer.position(buffer.position() + len);
            pos += len;
            return true;
        }

        @Override
        public void write(int b) {}

        @Override
        public void write(byte[] b) throws IOException {}

        @Override
        public void write(byte[] b, int off, int len) throws IOException {}

        @Override
        public void flush() {}

        @Override
        public void sync() throws IOException {}

        @Override
        public void close() throws IOException {}
    }
}
