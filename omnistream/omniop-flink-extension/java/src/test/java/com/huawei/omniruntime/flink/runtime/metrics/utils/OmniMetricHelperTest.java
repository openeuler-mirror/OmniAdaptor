/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
package com.huawei.omniruntime.flink.runtime.metrics.utils;

import com.huawei.omniruntime.flink.runtime.metrics.groups.OmniOperatorStateMetricGroup;
import com.huawei.omniruntime.flink.runtime.metrics.groups.OmniTaskLocalNettyBufferMetricGroup;
import com.huawei.omniruntime.flink.runtime.metrics.groups.OmniTaskMetricGroup;
import com.huawei.omniruntime.flink.runtime.metrics.groups.OmniTaskIOMetricGroup;
import com.huawei.omniruntime.flink.runtime.metrics.groups.VectorBatchBufferPoolMetricGroup;
import com.huawei.omniruntime.flink.runtime.metrics.OmniDescriptiveStatisticsHistogram;
import com.huawei.omniruntime.flink.runtime.metrics.OmniSimpleCounter;
import com.huawei.omniruntime.flink.runtime.metrics.OmniSizeGauge;
import com.huawei.omniruntime.flink.runtime.metrics.OmniTimeGauge;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.ViewUpdater;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.MetricNames;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

class OmniMetricHelperTest {
    @Test
    void memoryMetricRegistrationAndCleanupPreserveNativeIoBindings() {
        TaskMetricGroup task = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
        InternalOperatorIOMetricGroup operatorIO =
                task.getOrAddOperator(new OperatorID(), "Calc").getIOMetricGroup();
        TaskIOMetricGroup taskIO = task.getIOMetricGroup();
        SimpleCounter[] counters = {
            (SimpleCounter) operatorIO.getNumRecordsInCounter(),
            (SimpleCounter) operatorIO.getNumRecordsOutCounter(),
            (SimpleCounter) operatorIO.getNumBytesInCounter(),
            (SimpleCounter) operatorIO.getNumBytesOutCounter(),
            (SimpleCounter) taskIO.getNumBytesInCounter(),
            (SimpleCounter) taskIO.getNumBytesOutCounter()
        };
        // Inspect bindings without dereferencing the stand-in native addresses.
        for (int i = 0; i < counters.length; i++) {
            counters[i].setNativeRef(100L + i);
        }

        OmniTaskIOMetricGroup runtimeMetrics = mock(OmniTaskIOMetricGroup.class);
        try (MockedStatic<OmniMetricHelper> helper = mockStatic(OmniMetricHelper.class);
                MockedConstruction<OmniTaskLocalNettyBufferMetricGroup> netty =
                        mockConstruction(OmniTaskLocalNettyBufferMetricGroup.class);
                MockedConstruction<VectorBatchBufferPoolMetricGroup> batches =
                        mockConstruction(VectorBatchBufferPoolMetricGroup.class);
                MockedConstruction<OmniOperatorStateMetricGroup> states =
                        mockConstruction(OmniOperatorStateMetricGroup.class)) {
            helper.when(() -> OmniMetricHelper.registerTaskRuntimeMetrics(task, 1L)).thenReturn(runtimeMetrics);
            java.util.Map<String, OperatorID> operators = Collections.singletonMap("GroupAggregate", new OperatorID());
            helper.when(() -> OmniMetricHelper.registerOmniMetrics(task, 1L, 2L, operators)).thenCallRealMethod();
            OmniTaskMetricGroup memoryMetrics = OmniMetricHelper.registerOmniMetrics(
                    task, 1L, 2L, operators);

            assertSame(taskIO, task.getIOMetricGroup());
            assertSame(counters[0], operatorIO.getNumRecordsInCounter());
            assertSame(counters[1], operatorIO.getNumRecordsOutCounter());
            assertSame(counters[2], operatorIO.getNumBytesInCounter());
            assertSame(counters[3], operatorIO.getNumBytesOutCounter());
            assertSame(counters[4], taskIO.getNumBytesInCounter());
            assertSame(counters[5], taskIO.getNumBytesOutCounter());

            assertEquals(1, netty.constructed().size());
            assertEquals(1, batches.constructed().size());
            assertEquals(1, states.constructed().size());
            memoryMetrics.close();
            memoryMetrics.close();
            verify(netty.constructed().get(0)).close();
            verify(batches.constructed().get(0)).close();
            verify(states.constructed().get(0)).close();
            verify(runtimeMetrics, org.mockito.Mockito.times(2)).close();
            helper.verify(() -> OmniMetricHelper.registerTaskRuntimeMetrics(task, 1L));
            helper.verify(() -> OmniMetricHelper.registerTaskIOMetrics(task, 1L), never());
            helper.verify(() -> OmniMetricHelper.registerInternalOperatorMetric(task, 1L), never());
            for (int i = 0; i < counters.length; i++) {
                assertEquals(100L + i, counters[i].getNativeRef());
            }
        } finally {
            for (SimpleCounter counter : counters) {
                counter.setNativeRef(0L);
            }
            task.close();
        }
    }

    @Test
    void memoryMetricsPreserveTaskBoundaryCountsWhileChainedOperatorsProcessRecords() {
        TaskMetricGroup task = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
        InternalOperatorIOMetricGroup head =
                task.getOrAddOperator(new OperatorID(), "CalcHead").getIOMetricGroup();
        InternalOperatorIOMetricGroup middle =
                task.getOrAddOperator(new OperatorID(), "CalcMiddle").getIOMetricGroup();
        InternalOperatorIOMetricGroup tail =
                task.getOrAddOperator(new OperatorID(), "CalcTail").getIOMetricGroup();
        TaskIOMetricGroup taskIO = task.getIOMetricGroup();
        taskIO.reuseRecordsInputCounter(head.getNumRecordsInCounter());
        taskIO.reuseRecordsOutputCounter(tail.getNumRecordsOutCounter());
        head.getNumRecordsInCounter().inc(100);
        head.getNumRecordsOutCounter().inc(60);
        middle.getNumRecordsInCounter().inc(60);
        middle.getNumRecordsOutCounter().inc(40);
        tail.getNumRecordsInCounter().inc(40);
        tail.getNumRecordsOutCounter().inc(20);
        taskIO.getNumBytesInCounter().inc(1000);
        taskIO.getNumBytesOutCounter().inc(500);

        OmniTaskIOMetricGroup runtimeMetrics = mock(OmniTaskIOMetricGroup.class);
        try (MockedStatic<OmniMetricHelper> helper = mockStatic(OmniMetricHelper.class);
                MockedConstruction<OmniTaskLocalNettyBufferMetricGroup> netty =
                        mockConstruction(OmniTaskLocalNettyBufferMetricGroup.class);
                MockedConstruction<VectorBatchBufferPoolMetricGroup> batches =
                        mockConstruction(VectorBatchBufferPoolMetricGroup.class)) {
            helper.when(() -> OmniMetricHelper.registerTaskRuntimeMetrics(task, 1L)).thenReturn(runtimeMetrics);
            helper.when(() -> OmniMetricHelper.registerOmniMetrics(task, 1L, 2L, Collections.emptyMap()))
                    .thenCallRealMethod();
            OmniTaskMetricGroup memoryMetrics = OmniMetricHelper.registerOmniMetrics(
                    task, 1L, 2L, Collections.emptyMap());
            assertEquals(100, taskIO.getNumRecordsInCounter().getCount());
            assertEquals(20, taskIO.getNumRecordsOutCounter().getCount());
            assertEquals(1000, taskIO.getNumBytesInCounter().getCount());
            assertEquals(500, taskIO.getNumBytesOutCounter().getCount());

            head.getNumRecordsInCounter().inc(10);
            middle.getNumRecordsInCounter().inc(6);
            middle.getNumRecordsOutCounter().inc(4);
            tail.getNumRecordsOutCounter().inc(2);
            assertEquals(110, taskIO.getNumRecordsInCounter().getCount());
            assertEquals(22, taskIO.getNumRecordsOutCounter().getCount());
            memoryMetrics.close();
            assertEquals(110, taskIO.getNumRecordsInCounter().getCount());
            assertEquals(22, taskIO.getNumRecordsOutCounter().getCount());
        } finally {
            task.close();
        }
    }

    @Test
    void runtimeMetricsCreateRequiredTimersWithoutReplacingRecordOrByteCounters() {
        try (MockedStatic<OmniMetricHelper> helper = mockStatic(OmniMetricHelper.class)) {
            helper.when(() -> OmniMetricHelper.createOmniSimpleCounter(anyLong(), anyString(), anyString()))
                    .thenAnswer(call -> mock(OmniSimpleCounter.class));
            helper.when(() -> OmniMetricHelper.createOmniTimeGauge(anyLong(), anyString(), anyString()))
                    .thenAnswer(call -> mock(OmniTimeGauge.class));
            helper.when(() -> OmniMetricHelper.createSizeGauge(anyLong(), anyString(), anyString()))
                    .thenReturn(mock(OmniSizeGauge.class));
            helper.when(() -> OmniMetricHelper.createOmniOmniDescriptiveStatisticsHistogram(
                    anyLong(), eq(60), anyString(), anyString()))
                    .thenReturn(mock(OmniDescriptiveStatisticsHistogram.class));

            OmniTaskIOMetricGroup runtime = new OmniTaskIOMetricGroup(1L, false);
            assertTrue(runtime.getRegisteredMetrics().containsKey(MetricNames.TASK_HARD_BACK_PRESSURED_TIME));
            assertTrue(runtime.getRegisteredMetrics().containsKey(MetricNames.TASK_SOFT_BACK_PRESSURED_TIME));
            assertTrue(runtime.getRegisteredMetrics().containsKey(MetricNames.TASK_IDLE_TIME));
            for (String name : new String[] {MetricNames.IO_NUM_BYTES_IN, MetricNames.IO_NUM_BYTES_OUT,
                    MetricNames.IO_NUM_RECORDS_IN, MetricNames.IO_NUM_RECORDS_OUT,
                    MetricNames.IO_NUM_BYTES_IN_RATE, MetricNames.IO_NUM_BYTES_OUT_RATE,
                    MetricNames.IO_NUM_RECORDS_IN_RATE, MetricNames.IO_NUM_RECORDS_OUT_RATE}) {
                assertFalse(runtime.getRegisteredMetrics().containsKey(name), name);
            }
            helper.verify(() -> OmniMetricHelper.createOmniTimeGauge(
                    1L, "OmniTaskIOMetricGroup", MetricNames.TASK_HARD_BACK_PRESSURED_TIME));
            runtime.close();
        }
    }
    @Test
    void runtimeRegistrationAndCleanupPreserveQueryServiceIoCountersAndMeters() {
        TaskMetricGroup task = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
        Map<String, Metric> originalFields = OmniMetricHelper.getMetricMapFromProxyMetricGroup(
                task.getIOMetricGroup(), TaskIOMetricGroup.class);
        Map<Counter, Tuple2<QueryScopeInfo, String>> counters = new HashMap<>();
        Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges = new HashMap<>();
        Map<Meter, Tuple2<QueryScopeInfo, String>> meters = new HashMap<>();
        Map<Histogram, Tuple2<QueryScopeInfo, String>> histograms = new HashMap<>();
        Map<Metric, Tuple2<QueryScopeInfo, String>> originalIo = new HashMap<>();
        for (Map.Entry<String, Metric> entry : originalFields.entrySet()) {
            Metric metric = entry.getValue();
            Tuple2<QueryScopeInfo, String> scope = Tuple2.of(null, entry.getKey());
            if (metric instanceof Counter) {
                counters.put((Counter) metric, scope);
            } else if (metric instanceof Gauge) {
                gauges.put((Gauge<?>) metric, scope);
            } else if (metric instanceof Meter) {
                meters.put((Meter) metric, scope);
            } else if (metric instanceof Histogram) {
                histograms.put((Histogram) metric, scope);
            }
            if (entry.getKey().startsWith("numRecords") || entry.getKey().startsWith("numBytes")) {
                originalIo.put(metric, scope);
            }
        }
        assertEquals(8, originalIo.size());
        // Execute the shared registration overload while mocking the JNI factories and registry access.
        try (MockedStatic<OmniMetricHelper> helper = mockStatic(OmniMetricHelper.class, invocation -> {
            if (invocation.getMethod().getName().equals("registerTaskIOMetrics")
                    && invocation.getMethod().getParameterCount() == 3) {
                return invocation.callRealMethod();
            }
            return org.mockito.Answers.RETURNS_DEFAULTS.answer(invocation);
        })) {
            helper.when(() -> OmniMetricHelper.createOmniSimpleCounter(anyLong(), anyString(), anyString()))
                    .thenAnswer(call -> mock(OmniSimpleCounter.class));
            helper.when(() -> OmniMetricHelper.createOmniTimeGauge(anyLong(), anyString(), anyString()))
                    .thenAnswer(call -> mock(OmniTimeGauge.class));
            helper.when(() -> OmniMetricHelper.createSizeGauge(anyLong(), anyString(), anyString()))
                    .thenReturn(mock(OmniSizeGauge.class));
            helper.when(() -> OmniMetricHelper.createOmniOmniDescriptiveStatisticsHistogram(
                    anyLong(), eq(60), anyString(), anyString()))
                    .thenReturn(mock(OmniDescriptiveStatisticsHistogram.class));
            helper.when(() -> OmniMetricHelper.getMetricMapFromProxyMetricGroup(
                    task.getIOMetricGroup(), TaskIOMetricGroup.class)).thenCallRealMethod();
            helper.when(() -> OmniMetricHelper.getMetricFieldName(any(), any())).thenCallRealMethod();
            helper.when(() -> OmniMetricHelper.getCountersFromQueryService(task)).thenReturn(counters);
            helper.when(() -> OmniMetricHelper.getGaugesFromQueryService(task)).thenReturn(gauges);
            helper.when(() -> OmniMetricHelper.getMetersFromQueryService(task)).thenReturn(meters);
            helper.when(() -> OmniMetricHelper.getHistogramsFromQueryService(task)).thenReturn(histograms);
            helper.when(() -> OmniMetricHelper.getViewUpdater(task)).thenReturn(mock(ViewUpdater.class));
            helper.when(() -> OmniMetricHelper.updateOmniMetricsOnMetricQueryService(
                    eq(task), any(), anyMap(), anyMap())).thenCallRealMethod();
            helper.when(() -> OmniMetricHelper.removeOmniMetricsFromMetricQueryService(eq(task), anyList()))
                    .thenCallRealMethod();
            helper.when(() -> OmniMetricHelper.registerTaskRuntimeMetrics(task, 1L)).thenCallRealMethod();

            OmniTaskIOMetricGroup runtime = OmniMetricHelper.registerTaskRuntimeMetrics(task, 1L);
            assertTrue(runtime.getRegisteredMetrics().containsKey(MetricNames.TASK_HARD_BACK_PRESSURED_TIME));
            for (Map.Entry<Metric, Tuple2<QueryScopeInfo, String>> entry : originalIo.entrySet()) {
                Map<?, ?> registered = entry.getKey() instanceof Counter ? counters : meters;
                assertSame(entry.getValue(), registered.get(entry.getKey()));
            }
            OmniTaskMetricGroup memoryMetrics = new OmniTaskMetricGroup();
            memoryMetrics.setFlinkTaskMetricGroup(task);
            memoryMetrics.setOmniTaskIOMetricGroup(runtime);
            memoryMetrics.close();
            for (Map.Entry<Metric, Tuple2<QueryScopeInfo, String>> entry : originalIo.entrySet()) {
                Map<?, ?> registered = entry.getKey() instanceof Counter ? counters : meters;
                assertSame(entry.getValue(), registered.get(entry.getKey()));
            }
        } finally {
            task.close();
        }
    }

}
