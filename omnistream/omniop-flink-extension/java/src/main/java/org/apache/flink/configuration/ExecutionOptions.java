/*
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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

import static org.apache.flink.configuration.description.TextElement.text;

/** {@link ConfigOption}s specific for a single execution of a user program. */
@PublicEvolving
public class ExecutionOptions {
    /** A special marker value for disabling buffer timeout. */
    public static final long DISABLED_NETWORK_BUFFER_TIMEOUT = -1L;

    /** A special marker value for flushing network buffers after each record. */
    public static final long FLUSH_AFTER_EVERY_RECORD = 0L;

    public static final ConfigOption<RuntimeExecutionMode> RUNTIME_MODE =
            ConfigOptions.key("execution.runtime-mode")
                    .enumType(RuntimeExecutionMode.class)
                    .defaultValue(RuntimeExecutionMode.STREAMING)
                    .withDescription(
                            "Runtime execution mode of DataStream programs. Among other things, "
                                    + "this controls task scheduling, network shuffle behavior, and time semantics.");

    public static final ConfigOption<BatchShuffleMode> BATCH_SHUFFLE_MODE =
            ConfigOptions.key("execution.batch-shuffle-mode")
                    .enumType(BatchShuffleMode.class)
                    .defaultValue(BatchShuffleMode.ALL_EXCHANGES_BLOCKING)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines how data is exchanged between tasks in batch '%s' if the shuffling "
                                                    + "behavior has not been set explicitly for an individual exchange.",
                                            text(RUNTIME_MODE.key()))
                                    .linebreak()
                                    .text(
                                            "With pipelined exchanges, upstream and downstream tasks run simultaneously. "
                                                    + "In order to achieve lower latency, a result record is immediately "
                                                    + "sent to and processed by the downstream task. Thus, the receiver "
                                                    + "back-pressures the sender. The streaming mode always uses this "
                                                    + "exchange.")
                                    .linebreak()
                                    .text(
                                            "With blocking exchanges, upstream and downstream tasks run in stages. "
                                                    + "Records are persisted to some storage between stages. Downstream "
                                                    + "tasks then fetch these records after the upstream tasks finished. "
                                                    + "Such an exchange reduces the resources required to execute the "
                                                    + "job as it does not need to run upstream and downstream "
                                                    + "tasks simultaneously.")
                                    .linebreak()
                                    .text(
                                            "With hybrid exchanges (experimental), downstream tasks can run anytime as "
                                                    + "long as upstream tasks start running. When given sufficient "
                                                    + "resources, it can reduce the overall job execution time by running "
                                                    + "tasks simultaneously. Otherwise, it also allows jobs to be executed "
                                                    + "with very little resources. It adapts to custom preferences between "
                                                    + "persisting less data and restarting less tasks on failures, by "
                                                    + "providing different spilling strategies.")
                                    .build());

    /**
     * Should be moved to {@code ExecutionCheckpointingOptions} along with {@code
     * ExecutionConfig#useSnapshotCompression}, which should be put into {@code CheckpointConfig}.
     */
    public static final ConfigOption<Boolean> SNAPSHOT_COMPRESSION =
            ConfigOptions.key("execution.checkpointing.snapshot-compression")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Tells if we should use compression for the state snapshot data or not");

    public static final ConfigOption<Boolean> BUFFER_TIMEOUT_ENABLED =
            ConfigOptions.key("execution.buffer-timeout.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If disabled, the config execution.buffer-timeout.interval will not take effect and the flushing will be triggered only when the output "
                                                    + "buffer is full thus maximizing throughput")
                                    .build());

    public static final ConfigOption<Duration> BUFFER_TIMEOUT =
            ConfigOptions.key("execution.buffer-timeout.interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
                    .withDeprecatedKeys("execution.buffer-timeout")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The maximum time frequency (milliseconds) for the flushing of the output buffers. By default "
                                                    + "the output buffers flush frequently to provide low latency and to aid smooth developer "
                                                    + "experience. Setting the parameter can result in three logical modes:")
                                    .list(
                                            text(
                                                    "A positive value triggers flushing periodically by that interval"),
                                            text(
                                                    FLUSH_AFTER_EVERY_RECORD
                                                            + " triggers flushing after every record thus minimizing latency"),
                                            text(
                                                    "If the config "
                                                            + BUFFER_TIMEOUT_ENABLED.key()
                                                            + " is false,"
                                                            + " trigger flushing only when the output buffer is full thus maximizing "
                                                            + "throughput"))
                                    .build());

    @Documentation.ExcludeFromDocumentation(
            "This is an expert option, that we do not want to expose in the documentation")
    public static final ConfigOption<Boolean> SORT_INPUTS =
            ConfigOptions.key("execution.sorted-inputs.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "A flag to enable or disable sorting inputs of keyed operators. "
                                    + "NOTE: It takes effect only in the BATCH runtime mode.");

    @Documentation.ExcludeFromDocumentation(
            "This is an expert option, that we do not want to expose in the documentation")
    public static final ConfigOption<MemorySize> SORTED_INPUTS_MEMORY =
            ConfigOptions.key("execution.sorted-inputs.memory")
                    .memoryType()
                    // in sync with other weights from Table API and DataStream API
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription(
                            "Sets the managed memory size for sorting inputs of keyed operators in "
                                    + "BATCH runtime mode. The memory size is only a weight hint. "
                                    + "Thus, it will affect the operator's memory weight within a "
                                    + "task, but the actual memory used depends on the running "
                                    + "environment.");

    @Documentation.ExcludeFromDocumentation(
            "This is an expert option, that we do not want to expose in the documentation")
    public static final ConfigOption<Boolean> USE_BATCH_STATE_BACKEND =
            ConfigOptions.key("execution.batch-state-backend.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "A flag to enable or disable batch runtime specific state backend and timer service for keyed"
                                    + " operators. NOTE: It takes effect only in the BATCH runtime mode and requires sorted inputs"
                                    + SORT_INPUTS.key()
                                    + " to be enabled.");

    public static final ConfigOption<Boolean> LOAD_BASED_CHANNEL_SELECTOR_ENABLED =
            ConfigOptions.key("execution.load-based-channel-selector.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable load based strategy to distribute data, "
                                    + "when partitioner is RescalePartitioner or RebalancePartitioner.");

    public static final ConfigOption<String> LOAD_BASED_CHANNEL_SELECTOR_STRATEGY =
            ConfigOptions.key("execution.load-based-channel-selector.strategy")
                    .stringType()
                    .defaultValue("min")
                    .withDescription(
                            "The strategy of load-based-channel-selector. Default is min "
                                    + "for MinBacklogLoadBasedStrategy, otherwise threshold for ThresholdBacklogLoadBasedStrategy. "
                                    + "It works when enable the config execution.load-based-channel-selector.enable.");

    public static final ConfigOption<Double> CHANNEL_SELECTOR_STRATEGY_THRESHOLD_FACTOR =
            ConfigOptions.key("execution.load-based-channel-selector.strategy.threshold.factor")
                    .doubleType()
                    .defaultValue(1.3)
                    .withDescription(
                            "The factor of threshold. threshold = factor * average of backlogs. "
                                    + "It works when use threshold strategy.");

    public static final ConfigOption<Integer>
            CHANNEL_SELECTOR_STRATEGY_THRESHOLD_UPDATE_FREQUENCY_COUNT =
            ConfigOptions.key(
                            "execution.load-based-channel-selector.strategy.threshold.update-frequency-count")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The count of update threshold frequency. "
                                    + "It works when use threshold strategy.");

    public static final ConfigOption<Integer> CHANNEL_SELECTOR_STRATEGY_MIN_UPDATE_INTERVAL =
            ConfigOptions.key("execution.load-based-channel-selector.strategy.min.update-interval")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The interval of update load based strategy. "
                                    + "It works when the config execution.load-based-channel-selector.strategy is min.");

    public static final ConfigOption<Boolean> BACKLOG_METRICS_ENABLED =
            ConfigOptions.key("execution.backlog-metrics.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether use backlog metric or not.");

    public static final ConfigOption<Boolean> USE_HINT_PARALLELISM_FOR_SOURCE =
            ConfigOptions.key("execution.source.use-hint-parallelism")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to use hint parallelism instead of parallelism for source transformations. "
                                    + "When enabled, if hintParallelism is set (not 0), it will be used instead of parallelism. "
                                    + "By default, parallelism is used.");
}
