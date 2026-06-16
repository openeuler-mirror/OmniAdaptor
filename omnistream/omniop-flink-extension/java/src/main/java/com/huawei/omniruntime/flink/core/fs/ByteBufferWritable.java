/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */

package com.huawei.omniruntime.flink.core.fs;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Optional fast path for streams that can consume a {@link ByteBuffer} synchronously.
 *
 * <p>The implementation must consume bytes from the buffer before returning and advance the buffer
 * position by the number of written bytes. Callers may reuse the backing native memory immediately
 * after this method returns.
 *
 * @return {@code true} when the bytes were consumed through a ByteBuffer-native path, or {@code
 *     false} when the implementation had to copy into a heap byte array fallback.
 */
public interface ByteBufferWritable {
    boolean write(ByteBuffer buffer) throws IOException;
}
