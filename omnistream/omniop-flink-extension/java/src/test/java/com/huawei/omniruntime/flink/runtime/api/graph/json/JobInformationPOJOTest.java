/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.api.graph.json;

import com.huawei.omniruntime.flink.configuration.OmniRecoveryOptions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.util.SerializedValue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link JobInformationPOJO}. */
public class JobInformationPOJOTest {

    @Test
    @DisplayName("constructor copies recovery savepoint format from job configuration")
    void testConstructorCopiesRecoverySavepointFormat() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(
                OmniRecoveryOptions.RECOVERY_SAVEPOINT_FORMAT_CONFIG_NAME,
                OmniRecoveryOptions.RECOVERY_SAVEPOINT_FORMAT_FLINK_COMPATIBLE);
        JobInformation jobInformation = createJobInformation(configuration);

        JobInformationPOJO jobInformationPOJO =
                new JobInformationPOJO(jobInformation, getClass().getClassLoader());

        assertEquals(
                OmniRecoveryOptions.RECOVERY_SAVEPOINT_FORMAT_FLINK_COMPATIBLE,
                jobInformationPOJO.getRecoverySavepointFormat());
        assertTrue(
                jobInformationPOJO
                        .toString()
                        .contains(
                                "recoverySavepointFormat='"
                                        + OmniRecoveryOptions
                                                .RECOVERY_SAVEPOINT_FORMAT_FLINK_COMPATIBLE
                                        + "'"));
    }

    @Test
    @DisplayName("recovery savepoint format can be updated")
    void testSetRecoverySavepointFormat() {
        JobInformationPOJO jobInformationPOJO = new JobInformationPOJO();

        jobInformationPOJO.setRecoverySavepointFormat(
                OmniRecoveryOptions.RECOVERY_SAVEPOINT_FORMAT_OMNI_INTERNAL);

        assertEquals(
                OmniRecoveryOptions.RECOVERY_SAVEPOINT_FORMAT_OMNI_INTERNAL,
                jobInformationPOJO.getRecoverySavepointFormat());
    }

    private JobInformation createJobInformation(Configuration configuration) throws Exception {
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setAutoWatermarkInterval(100L);
        return new JobInformation(
                new JobID(),
                "test-job",
                new SerializedValue<>(executionConfig),
                configuration,
                Collections.emptyList(),
                Collections.emptyList());
    }
}
