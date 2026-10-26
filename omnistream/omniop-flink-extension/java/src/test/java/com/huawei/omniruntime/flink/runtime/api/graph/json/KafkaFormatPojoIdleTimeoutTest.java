/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
package com.huawei.omniruntime.flink.runtime.api.graph.json;

import com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.tablescan.KafkaFormatPojo;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaFormatPojoIdleTimeoutTest {
    @Test
    void preservesIdleTimeoutWhenCreatingNativeSourceDescription() {
        KafkaFormatPojo source = JsonHelper.fromJson(
                "{\"format\":\"kafka\",\"idleTimeout\":20000,\"watermarkStrategy\":\"bounded\"}",
                KafkaFormatPojo.class);
        source.setBatch(true);
        source.setEmitProgressiveWatermarks(true);

        String nativeDescription = JsonHelper.toJson(source);
        KafkaFormatPojo restored = JsonHelper.fromJson(nativeDescription, KafkaFormatPojo.class);
        assertEquals(20000L, restored.getIdleTimeout());
        assertTrue(nativeDescription.contains("\"idleTimeout\""));
        assertTrue(restored.isBatch());
        assertTrue(restored.isEmitProgressiveWatermarks());
    }

    @Test
    void missingIdleTimeoutKeepsIdlenessDisabled() {
        KafkaFormatPojo source = JsonHelper.fromJson("{\"format\":\"kafka\"}", KafkaFormatPojo.class);
        assertEquals(0L, source.getIdleTimeout());
    }
}
