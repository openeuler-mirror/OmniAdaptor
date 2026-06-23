package com.huawei.omniruntime.flink.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class HighParallelismOptimizeOptions {
    public static final ConfigOption<Boolean> HIGH_PARALLELISM_ENABLE =
            ConfigOptions.key("high-parallelism.optimize.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to High parallelism optimize");
}
