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
package org.apache.flink.runtime.checkpoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * StateAssignmentOperation 单元测试，覆盖 key group 分区、keyed state 提取、state 重分区等核心静态方法。
 */
public class StateAssignmentOperationTest {

    private static final OperatorID OPERATOR_ID = new OperatorID();

    /**
     * createKeyGroupPartitions 在 parallelism=1 时应返回单个覆盖全部 key group 的分区。
     */
    @Test
    void testCreateKeyGroupPartitionsSingleParallelism() {
        int maxParallelism = 128;
        int parallelism = 1;

        List<KeyGroupRange> partitions =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism, parallelism);

        assertEquals(1, partitions.size());
        assertEquals(0, partitions.get(0).getStartKeyGroup());
        assertEquals(maxParallelism - 1, partitions.get(0).getEndKeyGroup());
    }

    /**
     * createKeyGroupPartitions 在 parallelism=4 时应返回 4 个连续且不重叠的分区。
     */
    @Test
    void testCreateKeyGroupPartitionsMultipleParallelism() {
        int maxParallelism = 128;
        int parallelism = 4;

        List<KeyGroupRange> partitions =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism, parallelism);

        assertEquals(parallelism, partitions.size());

        assertEquals(0, partitions.get(0).getStartKeyGroup());
        for (int i = 1; i < partitions.size(); i++) {
            assertEquals(
                    partitions.get(i - 1).getEndKeyGroup() + 1,
                    partitions.get(i).getStartKeyGroup());
        }
        assertEquals(maxParallelism - 1, partitions.get(partitions.size() - 1).getEndKeyGroup());
    }

    /**
     * createKeyGroupPartitions 在 parallelism 等于 maxParallelism 时每个分区应只包含一个 key group。
     */
    @Test
    void testCreateKeyGroupPartitionsMaxParallelism() {
        int maxParallelism = 8;
        int parallelism = 8;

        List<KeyGroupRange> partitions =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism, parallelism);

        assertEquals(parallelism, partitions.size());
        for (int i = 0; i < partitions.size(); i++) {
            assertEquals(i, partitions.get(i).getStartKeyGroup());
            assertEquals(i, partitions.get(i).getEndKeyGroup());
        }
    }

    /**
     * extractIntersectingState 在有交集时应将交集 handle 加入 collector。
     */
    @Test
    void testExtractIntersectingStateWithIntersection() {
        KeyGroupRange handleRange = new KeyGroupRange(0, 63);
        KeyGroupRange extractRange = new KeyGroupRange(32, 95);

        StreamStateHandle streamHandle = new ByteStreamStateHandle("test", new byte[10]);
        KeyGroupsStateHandle stateHandle =
                new KeyGroupsStateHandle(new KeyGroupRangeOffsets(handleRange), streamHandle);

        List<KeyedStateHandle> handles = new ArrayList<>();
        handles.add(stateHandle);

        List<KeyedStateHandle> collector = new ArrayList<>();
        StateAssignmentOperation.extractIntersectingState(handles, extractRange, collector);

        assertEquals(1, collector.size());
        KeyedStateHandle intersected = collector.get(0);
        assertNotNull(intersected);
        assertEquals(32, intersected.getKeyGroupRange().getStartKeyGroup());
        assertEquals(63, intersected.getKeyGroupRange().getEndKeyGroup());
    }

    /**
     * extractIntersectingState 在无交集时不应向 collector 添加任何 handle。
     */
    @Test
    void testExtractIntersectingStateNoIntersection() {
        KeyGroupRange handleRange = new KeyGroupRange(0, 31);
        KeyGroupRange extractRange = new KeyGroupRange(64, 95);

        StreamStateHandle streamHandle = new ByteStreamStateHandle("test", new byte[10]);
        KeyGroupsStateHandle stateHandle =
                new KeyGroupsStateHandle(new KeyGroupRangeOffsets(handleRange), streamHandle);

        List<KeyedStateHandle> handles = new ArrayList<>();
        handles.add(stateHandle);

        List<KeyedStateHandle> collector = new ArrayList<>();
        StateAssignmentOperation.extractIntersectingState(handles, extractRange, collector);

        assertTrue(collector.isEmpty());
    }

    /**
     * extractIntersectingState 在输入包含 null 元素时应跳过 null 并正常处理。
     */
    @Test
    void testExtractIntersectingStateWithNullElement() {
        KeyGroupRange handleRange = new KeyGroupRange(0, 63);
        KeyGroupRange extractRange = new KeyGroupRange(0, 63);

        StreamStateHandle streamHandle = new ByteStreamStateHandle("test", new byte[10]);
        KeyGroupsStateHandle stateHandle =
                new KeyGroupsStateHandle(new KeyGroupRangeOffsets(handleRange), streamHandle);

        List<KeyedStateHandle> handles = new ArrayList<>();
        handles.add(null);
        handles.add(stateHandle);

        List<KeyedStateHandle> collector = new ArrayList<>();
        StateAssignmentOperation.extractIntersectingState(handles, extractRange, collector);

        assertEquals(1, collector.size());
    }

    /**
     * extractIntersectingState 在空集合输入时 collector 应保持为空。
     */
    @Test
    void testExtractIntersectingStateEmptyCollection() {
        List<KeyedStateHandle> handles = Collections.emptyList();
        List<KeyedStateHandle> collector = new ArrayList<>();

        StateAssignmentOperation.extractIntersectingState(
                handles, new KeyGroupRange(0, 63), collector);

        assertTrue(collector.isEmpty());
    }

    /**
     * getManagedKeyedStateHandles 在 operatorState 无 subtask state 时应返回空列表。
     */
    @Test
    void testGetManagedKeyedStateHandlesEmptyState() {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 2, 128);
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 63);

        List<KeyedStateHandle> result =
                StateAssignmentOperation.getManagedKeyedStateHandles(operatorState, keyGroupRange);

        assertTrue(result.isEmpty());
    }

    /**
     * getManagedKeyedStateHandles 在有交集时应返回匹配的 keyed state handles。
     */
    @Test
    void testGetManagedKeyedStateHandlesWithIntersection() {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 2, 128);

        StreamStateHandle streamHandle = new ByteStreamStateHandle("test", new byte[10]);
        KeyGroupsStateHandle keyedHandle =
                new KeyGroupsStateHandle(new KeyGroupRangeOffsets(new KeyGroupRange(0, 63)), streamHandle);

        OperatorSubtaskState subtaskState =
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(keyedHandle)
                        .build();
        operatorState.putState(0, subtaskState);

        KeyGroupRange extractRange = new KeyGroupRange(32, 95);
        List<KeyedStateHandle> result =
                StateAssignmentOperation.getManagedKeyedStateHandles(operatorState, extractRange);

        assertEquals(1, result.size());
        assertEquals(32, result.get(0).getKeyGroupRange().getStartKeyGroup());
        assertEquals(63, result.get(0).getKeyGroupRange().getEndKeyGroup());
    }

    /**
     * getRawKeyedStateHandles 在有交集时应返回匹配的 raw keyed state handles。
     */
    @Test
    void testGetRawKeyedStateHandlesWithIntersection() {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 2, 128);

        StreamStateHandle streamHandle = new ByteStreamStateHandle("test", new byte[10]);
        KeyGroupsStateHandle keyedHandle =
                new KeyGroupsStateHandle(new KeyGroupRangeOffsets(new KeyGroupRange(0, 63)), streamHandle);

        OperatorSubtaskState subtaskState =
                OperatorSubtaskState.builder()
                        .setRawKeyedState(keyedHandle)
                        .build();
        operatorState.putState(0, subtaskState);

        KeyGroupRange extractRange = new KeyGroupRange(0, 31);
        List<KeyedStateHandle> result =
                StateAssignmentOperation.getRawKeyedStateHandles(operatorState, extractRange);

        assertEquals(1, result.size());
    }

    /**
     * getRawKeyedStateHandles 在无交集时应返回空列表。
     */
    @Test
    void testGetRawKeyedStateHandlesNoIntersection() {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 2, 128);

        StreamStateHandle streamHandle = new ByteStreamStateHandle("test", new byte[10]);
        KeyGroupsStateHandle keyedHandle =
                new KeyGroupsStateHandle(new KeyGroupRangeOffsets(new KeyGroupRange(0, 31)), streamHandle);

        OperatorSubtaskState subtaskState =
                OperatorSubtaskState.builder()
                        .setRawKeyedState(keyedHandle)
                        .build();
        operatorState.putState(0, subtaskState);

        KeyGroupRange extractRange = new KeyGroupRange(64, 95);
        List<KeyedStateHandle> result =
                StateAssignmentOperation.getRawKeyedStateHandles(operatorState, extractRange);

        assertTrue(result.isEmpty());
    }

    /**
     * reDistributePartitionableStates 在空 operatorStates 时不应抛异常。
     */
    @Test
    void testReDistributePartitionableStatesEmpty() {
        Map<OperatorID, OperatorState> oldOperatorStates = new HashMap<>();
        Map<OperatorInstanceID, List<OperatorStateHandle>> result = new HashMap<>();

        StateAssignmentOperation.reDistributePartitionableStates(
                oldOperatorStates,
                2,
                OperatorSubtaskState::getManagedOperatorState,
                RoundRobinOperatorStateRepartitioner.INSTANCE,
                result);

        assertTrue(result.isEmpty());
    }

    /**
     * reDistributePartitionableStates 在有 operator state 时应正确重分区。
     */
    @Test
    void testReDistributePartitionableStatesWithData() {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 2, 128);

        ByteStreamStateHandle handle0 = new ByteStreamStateHandle("s0", new byte[] {0});
        ByteStreamStateHandle handle1 = new ByteStreamStateHandle("s1", new byte[] {1});

        OperatorSubtaskState subtaskState0 =
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(new StateObjectCollection<>(Collections.singletonList(new OperatorStreamStateHandle(Collections.emptyMap(), handle0))))
                        .build();
        OperatorSubtaskState subtaskState1 =
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(new StateObjectCollection<>(Collections.singletonList(new OperatorStreamStateHandle(Collections.emptyMap(), handle1))))
                        .build();
        operatorState.putState(0, subtaskState0);
        operatorState.putState(1, subtaskState1);

        Map<OperatorID, OperatorState> oldOperatorStates = new HashMap<>();
        oldOperatorStates.put(OPERATOR_ID, operatorState);

        Map<OperatorInstanceID, List<OperatorStateHandle>> result = new HashMap<>();

        StateAssignmentOperation.reDistributePartitionableStates(
                oldOperatorStates,
                2,
                OperatorSubtaskState::getManagedOperatorState,
                RoundRobinOperatorStateRepartitioner.INSTANCE,
                result);

        assertEquals(2, result.size());
        assertTrue(result.containsKey(OperatorInstanceID.of(0, OPERATOR_ID)));
        assertTrue(result.containsKey(OperatorInstanceID.of(1, OPERATOR_ID)));
    }

    /**
     * reDistributePartitionableStates 在扩容时应正确分配 state 到更多 subtask。
     */
    @Test
    void testReDistributePartitionableStatesScaleUp() {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 2, 128);

        ByteStreamStateHandle handle0 = new ByteStreamStateHandle("s0", new byte[] {0});
        ByteStreamStateHandle handle1 = new ByteStreamStateHandle("s1", new byte[] {1});

        OperatorSubtaskState subtaskState0 =
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(new StateObjectCollection<>(Collections.singletonList(new OperatorStreamStateHandle(Collections.emptyMap(), handle0))))
                        .build();
        OperatorSubtaskState subtaskState1 =
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(new StateObjectCollection<>(Collections.singletonList(new OperatorStreamStateHandle(Collections.emptyMap(), handle1))))
                        .build();
        operatorState.putState(0, subtaskState0);
        operatorState.putState(1, subtaskState1);

        Map<OperatorID, OperatorState> oldOperatorStates = new HashMap<>();
        oldOperatorStates.put(OPERATOR_ID, operatorState);

        Map<OperatorInstanceID, List<OperatorStateHandle>> result = new HashMap<>();

        StateAssignmentOperation.reDistributePartitionableStates(
                oldOperatorStates,
                4,
                OperatorSubtaskState::getManagedOperatorState,
                RoundRobinOperatorStateRepartitioner.INSTANCE,
                result);

        assertEquals(4, result.size());
    }

    /**
     * reDistributePartitionableStates 在缩容时应正确合并 state 到更少 subtask。
     */
    @Test
    void testReDistributePartitionableStatesScaleDown() {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 4, 128);

        for (int i = 0; i < 4; i++) {
            ByteStreamStateHandle handle =
                    new ByteStreamStateHandle("s" + i, new byte[] {(byte) i});
            OperatorSubtaskState subtaskState =
                    OperatorSubtaskState.builder()
                            .setManagedOperatorState(new StateObjectCollection<>(Collections.singletonList(new OperatorStreamStateHandle(Collections.emptyMap(), handle))))
                            .build();
            operatorState.putState(i, subtaskState);
        }

        Map<OperatorID, OperatorState> oldOperatorStates = new HashMap<>();
        oldOperatorStates.put(OPERATOR_ID, operatorState);

        Map<OperatorInstanceID, List<OperatorStateHandle>> result = new HashMap<>();

        StateAssignmentOperation.reDistributePartitionableStates(
                oldOperatorStates,
                2,
                OperatorSubtaskState::getManagedOperatorState,
                RoundRobinOperatorStateRepartitioner.INSTANCE,
                result);

        assertEquals(2, result.size());
    }

    /**
     * reDistributePartitionableStates 应同时支持 raw operator state 的重分区。
     */
    @Test
    void testReDistributePartitionableStatesRawOperatorState() {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 2, 128);

        ByteStreamStateHandle handle0 = new ByteStreamStateHandle("r0", new byte[] {0});
        ByteStreamStateHandle handle1 = new ByteStreamStateHandle("r1", new byte[] {1});

        OperatorSubtaskState subtaskState0 =
                OperatorSubtaskState.builder()
                        .setRawOperatorState(new StateObjectCollection<>(Collections.singletonList(new OperatorStreamStateHandle(Collections.emptyMap(), handle0))))
                        .build();
        OperatorSubtaskState subtaskState1 =
                OperatorSubtaskState.builder()
                        .setRawOperatorState(new StateObjectCollection<>(Collections.singletonList(new OperatorStreamStateHandle(Collections.emptyMap(), handle1))))
                        .build();
        operatorState.putState(0, subtaskState0);
        operatorState.putState(1, subtaskState1);

        Map<OperatorID, OperatorState> oldOperatorStates = new HashMap<>();
        oldOperatorStates.put(OPERATOR_ID, operatorState);

        Map<OperatorInstanceID, List<OperatorStateHandle>> result = new HashMap<>();

        StateAssignmentOperation.reDistributePartitionableStates(
                oldOperatorStates,
                2,
                OperatorSubtaskState::getRawOperatorState,
                RoundRobinOperatorStateRepartitioner.INSTANCE,
                result);

        assertEquals(2, result.size());
    }

    /**
     * getManagedKeyedStateHandles 在多个 subtask 都有 state 时应聚合所有交集。
     */
    @Test
    void testGetManagedKeyedStateHandlesMultipleSubtasks() {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 3, 128);

        StreamStateHandle streamHandle0 = new ByteStreamStateHandle("t0", new byte[10]);
        KeyGroupsStateHandle keyedHandle0 =
                new KeyGroupsStateHandle(new KeyGroupRangeOffsets(new KeyGroupRange(0, 42)), streamHandle0);

        StreamStateHandle streamHandle1 = new ByteStreamStateHandle("t1", new byte[10]);
        KeyGroupsStateHandle keyedHandle1 =
                new KeyGroupsStateHandle(new KeyGroupRangeOffsets(new KeyGroupRange(43, 85)), streamHandle1);

        StreamStateHandle streamHandle2 = new ByteStreamStateHandle("t2", new byte[10]);
        KeyGroupsStateHandle keyedHandle2 =
                new KeyGroupsStateHandle(new KeyGroupRangeOffsets(new KeyGroupRange(86, 127)), streamHandle2);

        operatorState.putState(
                0,
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(keyedHandle0)
                        .build());
        operatorState.putState(
                1,
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(keyedHandle1)
                        .build());
        operatorState.putState(
                2,
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(keyedHandle2)
                        .build());

        KeyGroupRange extractRange = new KeyGroupRange(30, 100);
        List<KeyedStateHandle> result =
                StateAssignmentOperation.getManagedKeyedStateHandles(operatorState, extractRange);

        assertEquals(3, result.size());
    }

    /**
     * getManagedKeyedStateHandles 在部分 subtask 无 state 时应只返回有交集的 handle。
     */
    @Test
    void testGetManagedKeyedStateHandlesPartialSubtasks() {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 3, 128);

        StreamStateHandle streamHandle = new ByteStreamStateHandle("t0", new byte[10]);
        KeyGroupsStateHandle keyedHandle =
                new KeyGroupsStateHandle(new KeyGroupRangeOffsets(new KeyGroupRange(0, 42)), streamHandle);

        operatorState.putState(
                0,
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(keyedHandle)
                        .build());

        KeyGroupRange extractRange = new KeyGroupRange(0, 127);
        List<KeyedStateHandle> result =
                StateAssignmentOperation.getManagedKeyedStateHandles(operatorState, extractRange);

        assertEquals(1, result.size());
    }

    /**
     * createKeyGroupPartitions 在 parallelism=2, maxParallelism=10 时应正确分配。
     */
    @Test
    void testCreateKeyGroupPartitionsUnevenDistribution() {
        int maxParallelism = 10;
        int parallelism = 3;

        List<KeyGroupRange> partitions =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism, parallelism);

        assertEquals(parallelism, partitions.size());
        assertEquals(0, partitions.get(0).getStartKeyGroup());
        assertEquals(maxParallelism - 1, partitions.get(partitions.size() - 1).getEndKeyGroup());

        int totalKeyGroups = 0;
        for (KeyGroupRange range : partitions) {
            totalKeyGroups += (range.getEndKeyGroup() - range.getStartKeyGroup() + 1);
        }
        assertEquals(maxParallelism, totalKeyGroups);
    }

    /**
     * extractIntersectingState 在 handle 完全包含在 extractRange 中时应返回完整 handle。
     */
    @Test
    void testExtractIntersectingStateFullyContained() {
        KeyGroupRange handleRange = new KeyGroupRange(32, 63);
        KeyGroupRange extractRange = new KeyGroupRange(0, 127);

        StreamStateHandle streamHandle = new ByteStreamStateHandle("test", new byte[10]);
        KeyGroupsStateHandle stateHandle =
                new KeyGroupsStateHandle(new KeyGroupRangeOffsets(handleRange), streamHandle);

        List<KeyedStateHandle> handles = new ArrayList<>();
        handles.add(stateHandle);

        List<KeyedStateHandle> collector = new ArrayList<>();
        StateAssignmentOperation.extractIntersectingState(handles, extractRange, collector);

        assertEquals(1, collector.size());
        assertEquals(32, collector.get(0).getKeyGroupRange().getStartKeyGroup());
        assertEquals(63, collector.get(0).getKeyGroupRange().getEndKeyGroup());
    }

    private static void invokeCheckParallelismPreconditions(
            OperatorState operatorState, ExecutionJobVertex executionJobVertex) throws Exception {
        Method method =
                StateAssignmentOperation.class.getDeclaredMethod(
                        "checkParallelismPreconditions",
                        OperatorState.class,
                        ExecutionJobVertex.class);
        method.setAccessible(true);
        try {
            method.invoke(null, operatorState, executionJobVertex);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }

    private static ExecutionJobVertex createMockExecutionJobVertex(
            int parallelism,
            int maxParallelism,
            boolean useOmni,
            int taskType,
            boolean canRescaleMaxParallelism) {
        JobVertexID jobVertexId = new JobVertexID();
        Configuration configuration = new Configuration();
        configuration.setBoolean("useomni", useOmni);
        configuration.setInteger("taskType", taskType);

        JobVertex jobVertex = Mockito.mock(JobVertex.class);
        when(jobVertex.getConfiguration()).thenReturn(configuration);

        ExecutionJobVertex executionJobVertex = Mockito.mock(ExecutionJobVertex.class);
        when(executionJobVertex.getJobVertex()).thenReturn(jobVertex);
        when(executionJobVertex.getJobVertexId()).thenReturn(jobVertexId);
        when(executionJobVertex.getParallelism()).thenReturn(parallelism);
        when(executionJobVertex.getMaxParallelism()).thenReturn(maxParallelism);
        when(executionJobVertex.canRescaleMaxParallelism(Mockito.anyInt()))
                .thenReturn(canRescaleMaxParallelism);

        return executionJobVertex;
    }

    /**
     * checkParallelismPreconditions 在并行度和 maxParallelism 均匹配时应正常通过。
     */
    @Test
    void testCheckParallelismPreconditionsNormal() throws Exception {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 4, 128);
        ExecutionJobVertex executionJobVertex =
                createMockExecutionJobVertex(4, 128, false, 0, false);

        assertDoesNotThrow(
                () -> invokeCheckParallelismPreconditions(operatorState, executionJobVertex));
    }

    /**
     * checkParallelismPreconditions 在 useOmni=true 且 taskType=1 且并行度不匹配时应抛出 IllegalStateException。
     */
    @Test
    void testCheckParallelismPreconditionsOmniTaskType1ParallelismMismatch() throws Exception {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 2, 128);
        ExecutionJobVertex executionJobVertex =
                createMockExecutionJobVertex(4, 128, true, 1, false);

        assertThrows(
                IllegalStateException.class,
                () -> invokeCheckParallelismPreconditions(operatorState, executionJobVertex));
    }

    /**
     * checkParallelismPreconditions 在 useOmni=true 但 taskType=0 时，即使并行度不匹配也不应抛异常。
     */
    @Test
    void testCheckParallelismPreconditionsOmniTaskType0ParallelismMismatch() throws Exception {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 2, 128);
        ExecutionJobVertex executionJobVertex =
                createMockExecutionJobVertex(4, 128, true, 0, false);

        assertDoesNotThrow(
                () -> invokeCheckParallelismPreconditions(operatorState, executionJobVertex));
    }

    /**
     * checkParallelismPreconditions 在 useOmni=false 时，即使 taskType=1 且并行度不匹配也不应抛异常。
     */
    @Test
    void testCheckParallelismPreconditionsNotOmniTaskType1ParallelismMismatch() throws Exception {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 2, 128);
        ExecutionJobVertex executionJobVertex =
                createMockExecutionJobVertex(4, 128, false, 1, false);

        assertDoesNotThrow(
                () -> invokeCheckParallelismPreconditions(operatorState, executionJobVertex));
    }

    /**
     * checkParallelismPreconditions 在恢复状态的 maxParallelism 低于当前 parallelism 时应抛出 IllegalStateException。
     */
    @Test
    void testCheckParallelismPreconditionsMaxParallelismLowerThanParallelism() throws Exception {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 2, 64);
        ExecutionJobVertex executionJobVertex =
                createMockExecutionJobVertex(128, 128, false, 0, false);

        assertThrows(
                IllegalStateException.class,
                () -> invokeCheckParallelismPreconditions(operatorState, executionJobVertex));
    }

    /**
     * checkParallelismPreconditions 在 maxParallelism 不匹配但可 rescale 时应正常通过并调用 setMaxParallelism。
     */
    @Test
    void testCheckParallelismPreconditionsMaxParallelismRescaleSuccess() throws Exception {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 4, 256);
        ExecutionJobVertex executionJobVertex =
                createMockExecutionJobVertex(4, 128, false, 0, true);

        assertDoesNotThrow(
                () -> invokeCheckParallelismPreconditions(operatorState, executionJobVertex));

        verify(executionJobVertex).setMaxParallelism(256);
    }

    /**
     * checkParallelismPreconditions 在 maxParallelism 不匹配且不可 rescale 时应抛出 IllegalStateException。
     */
    @Test
    void testCheckParallelismPreconditionsMaxParallelismRescaleFailure() throws Exception {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 4, 256);
        ExecutionJobVertex executionJobVertex =
                createMockExecutionJobVertex(4, 128, false, 0, false);

        assertThrows(
                IllegalStateException.class,
                () -> invokeCheckParallelismPreconditions(operatorState, executionJobVertex));

        verify(executionJobVertex, never()).setMaxParallelism(Mockito.anyInt());
    }

    /**
     * checkParallelismPreconditions 在 maxParallelism 相等时不应调用 setMaxParallelism。
     */
    @Test
    void testCheckParallelismPreconditionsMaxParallelismEqual() throws Exception {
        OperatorState operatorState = new OperatorState(OPERATOR_ID, 4, 128);
        ExecutionJobVertex executionJobVertex =
                createMockExecutionJobVertex(4, 128, false, 0, false);

        assertDoesNotThrow(
                () -> invokeCheckParallelismPreconditions(operatorState, executionJobVertex));

        verify(executionJobVertex, never()).setMaxParallelism(Mockito.anyInt());
    }
}
