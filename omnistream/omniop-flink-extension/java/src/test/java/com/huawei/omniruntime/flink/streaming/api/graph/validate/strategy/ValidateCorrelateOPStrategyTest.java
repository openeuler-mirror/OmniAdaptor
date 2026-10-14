/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValidateCorrelateOPStrategyTest {
    private final AbstractValidateOperatorStrategy strategy =
            ValidateOperatorStrategyFactory.getStrategy("Correlate");

    private Map<String, Object> description(String function, String joinType) {
        Map<String, Object> description = new HashMap<>();
        description.put("functionName", function);
        description.put("functionClass", "example.JsonSplit");
        description.put("joinType", joinType);
        description.put("inputTypes", Arrays.asList("BIGINT NOT NULL", "VARCHAR(255)"));
        description.put("outputTypes", Arrays.asList("BIGINT NOT NULL", "VARCHAR(255)", "VARCHAR"));
        return description;
    }

    @Test
    void supportsBothNativeFunctionsAndJoinTypesOnMaster() {
        for (String function : Arrays.asList("jsontest", "nrejsontest")) {
            for (String join : Arrays.asList("InnerJoin", "LeftOuterJoin")) {
                assertTrue(strategy.executeValidateOperator(description(function, join)));
            }
        }
    }

    @Test
    void fallsBackForUnsupportedFunctionsAndJoinTypes() {
        assertFalse(strategy.executeValidateOperator(description("unknown", "InnerJoin")));
        assertFalse(strategy.executeValidateOperator(description("jsontest", "RightOuterJoin")));
    }

    @Test
    void fallsBackForMissingMetadataAndUnsupportedTypes() {
        for (String field : Arrays.asList("functionName", "functionClass", "joinType", "inputTypes", "outputTypes")) {
            Map<String, Object> description = description("jsontest", "InnerJoin");
            description.remove(field);
            assertFalse(strategy.executeValidateOperator(description));
        }
        Map<String, Object> description = description("jsontest", "InnerJoin");
        description.put("inputTypes", Arrays.asList("MAP<STRING, STRING>"));
        assertFalse(strategy.executeValidateOperator(description));
    }
}
