/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs;

import com.huawei.omniruntime.flink.core.fs.ByteBufferWritable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingProxy;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Simple forwarding wrapper around {@link FSDataOutputStream}.
 *
 * <p>Adds {@link ByteBufferWritable} support to any output stream. This avoids
 * the JNI byte-array copy when OmniStream native code supplies savepoint data
 * through a DirectByteBuffer: the wrapper either delegates to the underlying
 * stream's ByteBufferWritable implementation (native path) or falls back to
 * heap-array copying.
 */
@Internal
public class FSDataOutputStreamWrapper extends FSDataOutputStream
        implements WrappingProxy<FSDataOutputStream>, ByteBufferWritable {

    protected final FSDataOutputStream outputStream;

    private byte[] byteBufferFallbackBuffer;

    public FSDataOutputStreamWrapper(FSDataOutputStream outputStream) {
        this.outputStream = Preconditions.checkNotNull(outputStream);
    }

    @Override
    public long getPos() throws IOException {
        return outputStream.getPos();
    }

    @Override
    public void flush() throws IOException {
        outputStream.flush();
    }

    @Override
    public void sync() throws IOException {
        outputStream.sync();
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        outputStream.write(b, off, len);
    }

    @Override
    public boolean write(ByteBuffer buffer) throws IOException {
        if (outputStream instanceof ByteBufferWritable) {
            return ((ByteBufferWritable) outputStream).write(buffer);
        }
        if (buffer.hasArray()) {
            int len = buffer.remaining();
            outputStream.write(buffer.array(), buffer.arrayOffset() + buffer.position(), len);
            buffer.position(buffer.position() + len);
            return false;
        }
        byte[] copyBuffer = getByteBufferFallbackBuffer(buffer.remaining());
        while (buffer.hasRemaining()) {
            int len = Math.min(buffer.remaining(), copyBuffer.length);
            buffer.get(copyBuffer, 0, len);
            outputStream.write(copyBuffer, 0, len);
        }
        return false;
    }

    private byte[] getByteBufferFallbackBuffer(int required) {
        if (byteBufferFallbackBuffer == null || byteBufferFallbackBuffer.length < required) {
            byteBufferFallbackBuffer = new byte[Math.min(required, 1024 * 1024)];
        }
        return byteBufferFallbackBuffer;
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    @Override
    public FSDataOutputStream getWrappedDelegate() {
        return outputStream;
    }
}
