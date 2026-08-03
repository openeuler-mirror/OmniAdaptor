/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.taskexecutor;

import com.huawei.omniruntime.flink.configuration.OmniRecoveryOptions;
import com.huawei.omniruntime.flink.streaming.api.graph.JobType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** Unit tests for recovery savepoint format validation in {@link OmniTaskExecutor}. */
public class OmniTaskExecutorTest {

    private OmniTaskExecutor taskExecutor;
    private Method checkJobTypeAndRecoverySavepointFormat;

    @BeforeEach
    void setUp() throws Exception {
        taskExecutor = mock(OmniTaskExecutor.class);
        checkJobTypeAndRecoverySavepointFormat =
                OmniTaskExecutor.class.getDeclaredMethod(
                        "checkJobTypeAndRecoverySavepointFormat", int.class, String.class);
        checkJobTypeAndRecoverySavepointFormat.setAccessible(true);
    }

    @Test
    @DisplayName("FLINK_COMPATIBLE is allowed for SQL jobs")
    void testCompatibleRecoverySavepointFormatIsAllowedForSqlJob() {
        assertDoesNotThrow(
                () ->
                        invokeCheck(
                                JobType.SQL,
                                OmniRecoveryOptions
                                        .RECOVERY_SAVEPOINT_FORMAT_FLINK_COMPATIBLE));
    }

    @Test
    @DisplayName("OMNI_INTERNAL is allowed for STREAM jobs")
    void testInternalRecoverySavepointFormatIsAllowedForStreamJob() {
        assertDoesNotThrow(
                () ->
                        invokeCheck(
                                JobType.STREAM,
                                OmniRecoveryOptions.RECOVERY_SAVEPOINT_FORMAT_OMNI_INTERNAL));
    }

    @Test
    @DisplayName("FLINK_COMPATIBLE is rejected for STREAM jobs")
    void testCompatibleRecoverySavepointFormatIsRejectedForStreamJob() {
        assertCompatibleFormatIsRejected(JobType.STREAM);
    }

    @Test
    @DisplayName("FLINK_COMPATIBLE is rejected for SQL_STREAM jobs")
    void testCompatibleRecoverySavepointFormatIsRejectedForSqlStreamJob() {
        assertCompatibleFormatIsRejected(JobType.SQL_STREAM);
    }

    private void assertCompatibleFormatIsRejected(JobType jobType) {
        InvocationTargetException exception =
                assertThrows(
                        InvocationTargetException.class,
                        () ->
                                invokeCheck(
                                        jobType,
                                        OmniRecoveryOptions
                                                .RECOVERY_SAVEPOINT_FORMAT_FLINK_COMPATIBLE));

        assertTrue(exception.getCause() instanceof IllegalArgumentException);
        assertTrue(
                exception
                        .getCause()
                        .getMessage()
                        .contains(OmniRecoveryOptions.RECOVERY_SAVEPOINT_FORMAT_CONFIG_NAME));
    }

    private void invokeCheck(JobType jobType, String recoverySavepointFormat) throws Exception {
        checkJobTypeAndRecoverySavepointFormat.invoke(
                taskExecutor, jobType.getValue(), recoverySavepointFormat);
    }
}
