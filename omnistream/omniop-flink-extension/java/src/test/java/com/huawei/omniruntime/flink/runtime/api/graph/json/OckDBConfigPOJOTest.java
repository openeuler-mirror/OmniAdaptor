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

package com.huawei.omniruntime.flink.runtime.api.graph.json;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OckDBConfigPOJOTest {
    @Test
    void testDefaultsMatchNativeContract() {
        OckDBConfigPOJO config = new OckDBConfigPOJO(new Configuration());
        JsonNode json = JsonHelper.getObjectMapper().valueToTree(config);

        assertEquals(4, json.get("checkpointTransferThreadNum").asInt());
        assertEquals("", json.get("backupDirectory").asText());
        assertEquals("", json.get("localDirectories").asText());
        assertEquals("HEAP", json.get("priorityQueueType").asText());
        assertEquals(20L * 1024L * 1024L, json.get("jniLogSizeBytes").asLong());
        assertEquals(0.8F, json.get("jniSliceWatermarkRatio").floatValue());
        assertEquals(0.2F, json.get("fileMemoryFraction").floatValue());
        assertEquals("lz4", json.get("lsmCompressionPolicy").asText());
        assertTrue(json.get("cacheFilterAndIndexSwitch").asBoolean());
        assertTrue(json.get("bloomFilterSwitch").asBoolean());
        assertFalse(json.get("kvSeparateSwitch").asBoolean());
    }

    @Test
    void testConfiguredValuesAreSerializedForNative() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setInteger("state.backend.ockdb.checkpoint.transfer.thread.num", 8);
        flinkConfig.setString("state.backend.ockdb.localdir", "/data/bss-a,/data/bss-b");
        flinkConfig.setString("state.backend.ockdb.timer-service.factory", "OCKDB");
        flinkConfig.setString("state.backend.ockdb.jni.logsize", "32mb");
        flinkConfig.setBoolean("state.backend.ockdb.ttl.filter.switch", true);
        flinkConfig.setBoolean("state.backend.ockdb.kv-separate.switch", true);
        flinkConfig.setInteger("state.backend.ockdb.kv-separate.threshold", 512);

        JsonNode json = JsonHelper.getObjectMapper().valueToTree(new OckDBConfigPOJO(flinkConfig));

        assertEquals(8, json.get("checkpointTransferThreadNum").asInt());
        assertEquals("/data/bss-a,/data/bss-b", json.get("localDirectories").asText());
        assertEquals("OCKDB", json.get("priorityQueueType").asText());
        assertEquals(32L * 1024L * 1024L, json.get("jniLogSizeBytes").asLong());
        assertTrue(json.get("ttlFilterSwitch").asBoolean());
        assertTrue(json.get("kvSeparateSwitch").asBoolean());
        assertEquals(512, json.get("kvSeparateThreshold").asInt());
    }

    @Test
    void testTaskInformationContainsVersionedResourceContract() {
        TaskInformationPOJO taskInfo = new TaskInformationPOJO();
        taskInfo.setStateBackendConfigVersion(1);
        taskInfo.setStateBackendResourceId(4294967295L);
        taskInfo.setOckDBConfig(new OckDBConfigPOJO(new Configuration()));

        JsonNode json = JsonHelper.getObjectMapper().valueToTree(taskInfo);
        assertEquals(1, json.get("stateBackendConfigVersion").asInt());
        assertEquals(4294967295L, json.get("stateBackendResourceId").asLong());
        assertTrue(json.has("ockDBConfig"));
    }

    @Test
    void testOckDBConfigValueEquality() {
        OckDBConfigPOJO left = new OckDBConfigPOJO(new Configuration());
        OckDBConfigPOJO right = new OckDBConfigPOJO(new Configuration());

        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());

        right.setKvSeparateThreshold(left.getKvSeparateThreshold() + 1);
        assertNotEquals(left, right);
    }

    @Test
    void testTaskInformationEqualityIncludesBackendContract() {
        TaskInformationPOJO left = new TaskInformationPOJO();
        left.setStateBackendConfigVersion(1);
        left.setStateBackendResourceId(100L);
        left.setOckDBConfig(new OckDBConfigPOJO(new Configuration()));

        TaskInformationPOJO right = new TaskInformationPOJO();
        right.setStateBackendConfigVersion(1);
        right.setStateBackendResourceId(100L);
        right.setOckDBConfig(new OckDBConfigPOJO(new Configuration()));

        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());

        right.setStateBackendResourceId(101L);
        assertNotEquals(left, right);
        right.setStateBackendResourceId(100L);
        right.getOckDBConfig().setKvSeparateThreshold(512);
        assertNotEquals(left, right);
    }
}
