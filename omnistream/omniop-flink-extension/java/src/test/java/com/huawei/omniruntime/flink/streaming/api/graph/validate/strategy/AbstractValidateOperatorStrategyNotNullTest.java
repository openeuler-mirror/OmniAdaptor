/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbstractValidateOperatorStrategyNotNullTest {

    private final AbstractValidateOperatorStrategy strategy = new ValidateAggOPStrategy();

    @Test
    void stripsOnlyTrailingNotNullSuffix() {
        assertEquals("BIGINT", AbstractValidateOperatorStrategy.stripNotNull("BIGINT NOT NULL"));
        assertEquals(
                "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                AbstractValidateOperatorStrategy.stripNotNull(
                        "TIMESTAMP_WITHOUT_TIME_ZONE(3) NOT NULL"));
        assertEquals("BIGINT", AbstractValidateOperatorStrategy.stripNotNull("BIGINT"));
    }

    @Test
    void validatesSupportedNonNullableTypes() {
        assertTrue(
                strategy.validateDataTypes(
                        Collections.singletonList(
                                Arrays.asList(
                                        "BIGINT NOT NULL",
                                        "VARCHAR(255) NOT NULL",
                                        "TIMESTAMP_WITHOUT_TIME_ZONE(3) NOT NULL"))));
    }
}
