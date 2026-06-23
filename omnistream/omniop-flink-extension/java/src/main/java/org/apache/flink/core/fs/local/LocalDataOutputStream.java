/*
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

package org.apache.flink.core.fs.local;

import com.huawei.omniruntime.flink.core.fs.ByteBufferWritable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FSDataOutputStream;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

/**
 * Local file system output stream with {@link ByteBufferWritable} support.
 *
 * <p>Uses {@link FileChannel#write(ByteBuffer)} for zero-copy writes from
 * DirectByteBuffers, bypassing heap-array copies during savepoint output.
 * Overrides the standard Flink {@link org.apache.flink.core.fs.local.LocalDataOutputStream}
 * to add the ByteBufferWritable fast path.
 */
@Internal
public class LocalDataOutputStream extends FSDataOutputStream implements ByteBufferWritable {

    /** The file output stream used to write data. */
    private final FileOutputStream fos;

    private boolean closed = false;

    /**
     * Constructs a new <code>LocalDataOutputStream</code> object from a given {@link File} object.
     *
     * @param file the {@link File} object the data stream is read from
     * @throws IOException thrown if the data output stream cannot be created
     */
    public LocalDataOutputStream(final File file) throws IOException {
        this.fos = new FileOutputStream(file);
    }

    @Override
    public void write(final int b) throws IOException {
        checkOpen();
        fos.write(b);
    }

    @Override
    public void write(@Nonnull final byte[] b) throws IOException {
        checkOpen();
        fos.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        checkOpen();
        fos.write(b, off, len);
    }

    @Override
    public boolean write(final ByteBuffer buffer) throws IOException {
        checkOpen();
        while (buffer.hasRemaining()) {
            fos.getChannel().write(buffer);
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        fos.close();
    }

    @Override
    public void flush() throws IOException {
        checkOpen();
        fos.flush();
    }

    @Override
    public void sync() throws IOException {
        checkOpen();
        fos.getFD().sync();
    }

    @Override
    public long getPos() throws IOException {
        checkOpen();
        return fos.getChannel().position();
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new ClosedChannelException();
        }
    }
}
