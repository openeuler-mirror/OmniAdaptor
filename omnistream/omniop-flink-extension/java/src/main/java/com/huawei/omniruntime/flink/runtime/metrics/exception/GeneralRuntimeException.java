/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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

package com.huawei.omniruntime.flink.runtime.metrics.exception;

/**
 * GeneralRuntimeException is a custom runtime exception class that extends RuntimeException.
 * It provides constructors to create exceptions with a message, a cause, or both.
 * This class is used to handle general runtime errors in the application.
 *
 * @since 2025-04-29
 */
public class GeneralRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public GeneralRuntimeException(String message) {
        super(message);
    }

    public GeneralRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public GeneralRuntimeException(Throwable cause) {
        super(cause);
    }
}
