/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

package org.apache.flink.streaming.api.graph;

import com.huawei.omniruntime.flink.configuration.OmniRecoveryOptions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for recovery savepoint format propagation to the generated {@link JobGraph}. */
public class StreamingJobGraphGeneratorRecoverySavepointFormatTest {

    @Test
    @DisplayName("default recovery savepoint format is written to JobGraph")
    void testDefaultRecoverySavepointFormatIsWrittenToJobGraph() {
        JobGraph jobGraph = createJobGraph(new Configuration());

        assertEquals(
                OmniRecoveryOptions.RECOVERY_SAVEPOINT_FORMAT_OMNI_INTERNAL,
                jobGraph
                        .getJobConfiguration()
                        .getString(OmniRecoveryOptions.RECOVERY_SAVEPOINT_FORMAT_CONFIG_NAME, null));
    }

    @Test
    @DisplayName("compatible recovery savepoint format is written to JobGraph")
    void testCompatibleRecoverySavepointFormatIsWrittenToJobGraph() {
        Configuration configuration = new Configuration();
        configuration.setString(
                OmniRecoveryOptions.RECOVERY_SAVEPOINT_FORMAT_CONFIG_NAME, "compatible");

        JobGraph jobGraph = createJobGraph(configuration);

        assertEquals(
                OmniRecoveryOptions.RECOVERY_SAVEPOINT_FORMAT_FLINK_COMPATIBLE,
                jobGraph
                        .getJobConfiguration()
                        .getString(OmniRecoveryOptions.RECOVERY_SAVEPOINT_FORMAT_CONFIG_NAME, null));
    }

    @Test
    @DisplayName("unsupport recovery savepoint format will cause exception")
    void testUnsupportRecoverySavepointFormatCauseException() {
        Configuration configuration = new Configuration();
        configuration.setString(
                OmniRecoveryOptions.RECOVERY_SAVEPOINT_FORMAT_CONFIG_NAME, "unsupport");

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                        createJobGraph(configuration);
                }
        );
    }

    private JobGraph createJobGraph(Configuration configuration) {
        StreamGraph streamGraph =
                new StreamGraph(
                        new ExecutionConfig(),
                        new CheckpointConfig(),
                        SavepointRestoreSettings.none());
        streamGraph.setJobName("recovery-savepoint-format-test");
        streamGraph.setConfiguration(configuration);
        return StreamingJobGraphGenerator.createJobGraph(streamGraph);
    }
}
