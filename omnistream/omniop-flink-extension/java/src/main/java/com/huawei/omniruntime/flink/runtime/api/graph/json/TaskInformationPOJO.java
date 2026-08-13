/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.huawei.omniruntime.flink.runtime.api.graph.json;

import static org.apache.flink.util.Preconditions.checkState;

import com.huawei.omniruntime.flink.runtime.api.graph.json.configuration.StreamConfigHelper;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBMemoryConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.util.TernaryBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * TaskInformationPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class TaskInformationPOJO {
    private static final Logger LOG = LoggerFactory.getLogger(TaskInformationPOJO.class);

    StreamConfigPOJO streamConfig;
    List<StreamConfigPOJO> chainedConfig;
    private String taskName;

    /**
     * The number of subtasks for this operator.
     */
    private int numberOfSubtasks;

    /**
     * The maximum parallelism == number of key groups.
     */
    private int maxNumberOfSubtasks;
    private int indexOfSubtask;

    private String stateBackend;

    // rocksdb related config
    private String[] rocksdbStorePaths = new String[0];
    private RocksDBMemoryConfiguration rocksDBMemoryConfiguration = new RocksDBMemoryConfiguration();
    private int numberOfTransferThreads = 4;
    private double stateBackendManagedMemoryFraction;
    private long stateBackendManagedMemorySize;
    private int stateBackendConfigVersion = 1;
    private long stateBackendResourceId;
    private long cacheAddr;
    private long writeBufferManagerAddr;
    private boolean splitWatermark = false;
    private String priorityQueueStateType = "";

    // ockdb related config，仅当stateBackend为EmbeddedOckStateBackend时填充，下传C++侧
    private OckDBConfigPOJO ockDBConfig;

    private int taskType;

    private CheckpointConfigPOJO checkpointConfig;

    private ExecutionCheckpointConfigPOJO executionCheckpointConfig;

    private String tmpWorkingDirectory = "";

    private String localRecoveryConfig = "";

    // Default constructor
    public TaskInformationPOJO() {
    }

    public TaskInformationPOJO(TaskInformation taskInformation, ClassLoader cl, int indexOfSubtask,
                               TaskManagerConfiguration taskManagerConfiguration, MemoryManager memoryManager) throws Exception {
        this.taskName = taskInformation.getTaskName();
        this.numberOfSubtasks = taskInformation.getNumberOfSubtasks();
        this.maxNumberOfSubtasks = taskInformation.getMaxNumberOfSubtasks();
        this.indexOfSubtask = indexOfSubtask;
        StreamConfig tempStreamConfig = new StreamConfig(taskInformation.getTaskConfiguration());
        this.streamConfig = new StreamConfigPOJO(tempStreamConfig, cl);

        getStateBackendConfig(tempStreamConfig, taskManagerConfiguration, cl, memoryManager);

        this.chainedConfig = new ArrayList<>(StreamConfigHelper.retrieveChainedConfig(tempStreamConfig, cl).values());
        getCheckpointConfig(tempStreamConfig);
        getTmpWorkDir(taskManagerConfiguration);
    }

    // Full argument constructor
    public TaskInformationPOJO(String taskName, int numberOfSubtasks, int maxNumberOfSubtasks, int indexOfSubtask,
                               StreamConfigPOJO streamConfig,
                               List<StreamConfigPOJO> chainedConfig) {
        this.taskName = taskName;
        this.numberOfSubtasks = numberOfSubtasks;
        this.maxNumberOfSubtasks = maxNumberOfSubtasks;
        this.indexOfSubtask = indexOfSubtask;
        this.streamConfig = streamConfig;
        this.chainedConfig = chainedConfig;
    }

    private void getStateBackendConfig(StreamConfig tempStreamConfig, TaskManagerConfiguration taskManagerConfiguration,
                                        ClassLoader cl, MemoryManager memoryManager) throws Exception {
        StateBackend applicationBackend = tempStreamConfig.getStateBackend(cl);
        StateBackend backend = StateBackendLoader.fromApplicationOrConfigOrDefault(
                applicationBackend,
                TernaryBoolean.UNDEFINED,
                taskManagerConfiguration.getConfiguration(),
                cl,
                LOG);
        stateBackend = backend.getClass().getSimpleName();
        if (stateBackend.equals("EmbeddedOckStateBackend")) {
            getOckDBStateBackendConfig(backend, tempStreamConfig, taskManagerConfiguration, cl, memoryManager);
            stateBackendResourceId = Integer.toUnsignedLong(System.identityHashCode(memoryManager));
            if (stateBackendResourceId == 0L) {
                stateBackendResourceId = 1L;
            }
            return;
        }
        if (stateBackend.equals("HashMapStateBackend")) {
            return;
        }
        if (!stateBackend.equals("EmbeddedRocksDBStateBackend")) {
            throw new UnsupportedOperationException("Unsupported native state backend: " + stateBackend);
        }
        Class<?> backendClass = backend.getClass();
        Field[] fields = backendClass.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }

            if (name.equals("localRocksDbDirectories")) {
                Object value = field.get(backend);
                File[] localRocksDbDirectories = (File[]) value;
                rocksdbStorePaths = getDbStoragePaths(localRocksDbDirectories);
            } else if (name.equals("numberOfTransferThreads")) {
                numberOfTransferThreads = (int) field.get(backend);
            } else if (name.equals("memoryConfiguration")) {
                rocksDBMemoryConfiguration = (RocksDBMemoryConfiguration) field.get(backend);
            }
        }

        populateManagedMemory(tempStreamConfig, taskManagerConfiguration, cl, memoryManager);

        priorityQueueStateType = ((EmbeddedRocksDBStateBackend) backend).getPriorityQueueStateType().toString();
    }

    private void populateManagedMemory(StreamConfig tempStreamConfig,
                                       TaskManagerConfiguration taskManagerConfiguration,
                                       ClassLoader cl,
                                       MemoryManager memoryManager) {
        stateBackendManagedMemoryFraction = tempStreamConfig.getManagedMemoryFractionOperatorUseCaseOfSlot(
                ManagedMemoryUseCase.STATE_BACKEND,
                taskManagerConfiguration.getConfiguration(),
                cl
        );

        if (stateBackendManagedMemoryFraction == 0.0) {
            LOG.warn("stateBackendManagedMemoryFraction is 0, maybe state backend {} is not used in task {}.",
                    stateBackend, taskName);
        } else {
            stateBackendManagedMemorySize = memoryManager.computeMemorySize(stateBackendManagedMemoryFraction);
        }
    }

    /**
     * 解析EmbeddedOckStateBackend的配置，参考OmniStateStore EmbeddedOckStateBackend的configure方法：
     * 优先使用backend实例已解析的字段（UNDEFINED视为未解析），未解析项从Flink ReadableConfig读取。
     * OckDB配置项整体通过OckDBConfigPOJO下传C++侧。
     */
    private void getOckDBStateBackendConfig(StateBackend backend, StreamConfig tempStreamConfig,
                                            TaskManagerConfiguration taskManagerConfiguration, ClassLoader cl,
                                            MemoryManager memoryManager) throws Exception {
        // 从Flink配置解析全部OckDB选项，配置key与OmniStateStore OckDBOptions一致
        Configuration backendConfiguration = getFieldValue(backend, "config", Configuration.class);
        ockDBConfig = new OckDBConfigPOJO(
                backendConfiguration == null ? taskManagerConfiguration.getConfiguration() : backendConfiguration);

        Class<?> backendClass = backend.getClass();
        Field[] fields = backendClass.getDeclaredFields();
        String[] resolvedLocalDirs = null;
        int resolvedTransferThreads = -1;
        String resolvedPqType = null;
        for (Field field : fields) {
            String name = field.getName();
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
            try {
                if (name.equals("localOckDbDirectories")) {
                    Object value = field.get(backend);
                    if (value instanceof File[]) {
                        resolvedLocalDirs = getDbStoragePaths((File[]) value);
                    }
                } else if (name.equals("numberOfTransferThreads")) {
                    resolvedTransferThreads = (int) field.get(backend);
                } else if (name.equals("priorityQueueStateType")) {
                    Object value = field.get(backend);
                    if (value != null) {
                        resolvedPqType = value.toString();
                    }
                }
            } catch (Exception e) {
                LOG.warn("reflect OckDB field {} failed: {}", name, e.getMessage());
            }
        }

        // UNDEFINED_NUMBER_OF_TRANSFER_THREADS = -1，未解析时用config默认值
        if (resolvedTransferThreads > 0) {
            numberOfTransferThreads = resolvedTransferThreads;
            ockDBConfig.setCheckpointTransferThreadNum(resolvedTransferThreads);
        } else {
            numberOfTransferThreads = ockDBConfig.getCheckpointTransferThreadNum();
        }

        // localDirectories优先用backend实例解析值，其次用config值
        if (resolvedLocalDirs != null && resolvedLocalDirs.length > 0) {
            rocksdbStorePaths = resolvedLocalDirs;
            ockDBConfig.setLocalDirectories(String.join(",", resolvedLocalDirs));
        } else if (ockDBConfig.getLocalDirectories() != null && !ockDBConfig.getLocalDirectories().isEmpty()) {
            rocksdbStorePaths = ockDBConfig.getLocalDirectories().split(",|" + File.pathSeparator);
        }

        // priorityQueueType优先用backend实例解析值，其次用config值
        if (resolvedPqType != null) {
            priorityQueueStateType = resolvedPqType;
            ockDBConfig.setPriorityQueueType(resolvedPqType);
        } else {
            priorityQueueStateType = ockDBConfig.getPriorityQueueType();
        }

        // 托管内存比例计算与RocksDB一致
        populateManagedMemory(tempStreamConfig, taskManagerConfiguration, cl, memoryManager);
    }

    private <T> T getFieldValue(Object target, String fieldName, Class<T> fieldType) throws IllegalAccessException {
        Class<?> currentClass = target.getClass();
        while (currentClass != null) {
            try {
                Field field = currentClass.getDeclaredField(fieldName);
                if (!field.isAccessible()) {
                    field.setAccessible(true);
                }
                Object value = field.get(target);
                return value == null ? null : fieldType.cast(value);
            } catch (NoSuchFieldException ignored) {
                currentClass = currentClass.getSuperclass();
            }
        }
        return null;
    }

    private String[] getDbStoragePaths(File[] localRocksDbDirectories) {
        if (localRocksDbDirectories == null) {
            return new String[0];
        } else {
            String[] paths = new String[localRocksDbDirectories.length];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = localRocksDbDirectories[i].toString();
            }
            return paths;
        }
    }

    private void getCheckpointConfig(StreamConfig tempStreamConfig) {
        this.checkpointConfig = JsonHelper.fromJson(tempStreamConfig.getCheckpointConf(), CheckpointConfigPOJO.class);
        this.executionCheckpointConfig =
            JsonHelper.fromJson(tempStreamConfig.getExecutionCheckpointConf(), ExecutionCheckpointConfigPOJO.class);
    }

    private void getTmpWorkDir(TaskManagerConfiguration taskManagerConfiguration) {
        try {
            this.tmpWorkingDirectory = taskManagerConfiguration.getTmpWorkingDirectory().getCanonicalPath();
        } catch (IOException ex) {
            LOG.warn("get tmpWorkingDirectory from taskManagerConfiguration error", ex);
        }
    }

    public boolean getSplitWatermark() {
        return splitWatermark;
    }

    public void setSplitWatermark(boolean splitWatermark) {
        this.splitWatermark = splitWatermark;
    }

    // Getters and setters
    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public int getNumberOfSubtasks() {
        return numberOfSubtasks;
    }

    public void setNumberOfSubtasks(int numberOfSubtasks) {
        this.numberOfSubtasks = numberOfSubtasks;
    }

    public int getMaxNumberOfSubtasks() {
        return maxNumberOfSubtasks;
    }

    public void setMaxNumberOfSubtasks(int maxNumberOfSubtasks) {
        this.maxNumberOfSubtasks = maxNumberOfSubtasks;
    }

    public StreamConfigPOJO getStreamConfig() {
        return streamConfig;
    }

    public void setStreamConfig(StreamConfigPOJO streamConfig) {
        this.streamConfig = streamConfig;
    }

    public int getIndexOfSubtask() {
        return indexOfSubtask;
    }

    public void setIndexOfSubtask(int indexOfSubtask) {
        this.indexOfSubtask = indexOfSubtask;
    }

    public List<StreamConfigPOJO> getChainedConfig() {
        return chainedConfig;
    }

    public void setChainedConfig(List<StreamConfigPOJO> chainedConfig) {
        this.chainedConfig = chainedConfig;
    }

    public String getStateBackend() {
        return stateBackend;
    }

    public void setStateBackend(String stateBackend) {
        this.stateBackend = stateBackend;
    }

    public String[] getRocksdbStorePaths() {
        return rocksdbStorePaths;
    }

    public void setRocksdbStorePaths(String[] rocksdbStorePaths) {
        this.rocksdbStorePaths = rocksdbStorePaths;
    }

    public int getNumberOfTransferThreads() {
        return numberOfTransferThreads;
    }

    public void setNumberOfTransferThreads(int numberOfTransferThreads) {
        this.numberOfTransferThreads = numberOfTransferThreads;
    }

    public RocksDBMemoryConfiguration getRocksDBMemoryConfiguration() {
        return rocksDBMemoryConfiguration;
    }

    public void setRocksDBMemoryConfiguration(RocksDBMemoryConfiguration rocksDBMemoryConfiguration) {
        this.rocksDBMemoryConfiguration = rocksDBMemoryConfiguration;
    }

    public double getStateBackendManagedMemoryFraction() {
        return stateBackendManagedMemoryFraction;
    }

    public void setStateBackendManagedMemoryFraction(double stateBackendManagedMemoryFraction) {
        this.stateBackendManagedMemoryFraction = stateBackendManagedMemoryFraction;
    }

    public long getStateBackendManagedMemorySize() {
        return stateBackendManagedMemorySize;
    }

    public void setStateBackendManagedMemorySize(long stateBackendManagedMemorySize) {
        this.stateBackendManagedMemorySize = stateBackendManagedMemorySize;
    }

    public int getStateBackendConfigVersion() {
        return stateBackendConfigVersion;
    }

    public void setStateBackendConfigVersion(int stateBackendConfigVersion) {
        this.stateBackendConfigVersion = stateBackendConfigVersion;
    }

    public long getStateBackendResourceId() {
        return stateBackendResourceId;
    }

    public void setStateBackendResourceId(long stateBackendResourceId) {
        this.stateBackendResourceId = stateBackendResourceId;
    }

    public long getWriteBufferManagerAddr() {
        return writeBufferManagerAddr;
    }

    public void setWriteBufferManagerAddr(long writeBufferManagerAddr) {
        this.writeBufferManagerAddr = writeBufferManagerAddr;
    }

    public long getCacheAddr() {
        return cacheAddr;
    }

    public void setCacheAddr(long cacheAddr) {
        this.cacheAddr = cacheAddr;
    }

    public String getPriorityQueueStateType() {
        return priorityQueueStateType;
    }

    public void setPriorityQueueStateType(String priorityQueueStateType) {
        this.priorityQueueStateType = priorityQueueStateType;
    }

    public int getTaskType() {
        return taskType;
    }
    public void setTaskType(int taskType) {
        this.taskType = taskType;
    }

    public CheckpointConfigPOJO getCheckpointConfig() {
        return checkpointConfig;
    }

    public void setCheckpointConfig(CheckpointConfigPOJO checkpointConfig) {
        this.checkpointConfig = checkpointConfig;
    }

    public ExecutionCheckpointConfigPOJO getExecutionCheckpointConfig() {
        return executionCheckpointConfig;
    }

    public void setExecutionCheckpointConfig(ExecutionCheckpointConfigPOJO executionCheckpointConfig) {
        this.executionCheckpointConfig = executionCheckpointConfig;
    }

    public String getTmpWorkingDirectory() {
        return tmpWorkingDirectory;
    }

    public void setTmpWorkingDirectory(String tmpWorkingDirectory) {
        this.tmpWorkingDirectory = tmpWorkingDirectory;
    }

    public String getLocalRecoveryConfig() {
        return localRecoveryConfig;
    }

    public void setLocalRecoveryConfig(String localRecoveryConfig) {
        this.localRecoveryConfig = localRecoveryConfig;
    }

    public OckDBConfigPOJO getOckDBConfig() {
        return ockDBConfig;
    }

    public void setOckDBConfig(OckDBConfigPOJO ockDBConfig) {
        this.ockDBConfig = ockDBConfig;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        checkState(o instanceof TaskInformationPOJO);
        TaskInformationPOJO that = (TaskInformationPOJO) o;
        return numberOfSubtasks == that.numberOfSubtasks
                && maxNumberOfSubtasks == that.maxNumberOfSubtasks
                && stateBackendConfigVersion == that.stateBackendConfigVersion
                && stateBackendResourceId == that.stateBackendResourceId
                && Objects.equals(taskName, that.taskName)
                && Objects.equals(streamConfig, that.streamConfig)
                && Objects.equals(chainedConfig, that.chainedConfig)
                && Objects.equals(indexOfSubtask, that.indexOfSubtask)
                && Objects.equals(localRecoveryConfig, that.localRecoveryConfig)
                && Objects.equals(ockDBConfig, that.ockDBConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                taskName,
                numberOfSubtasks,
                maxNumberOfSubtasks,
                indexOfSubtask,
                streamConfig,
                chainedConfig,
                localRecoveryConfig,
                stateBackendConfigVersion,
                stateBackendResourceId,
                ockDBConfig);
    }

    @Override
    public String toString() {
        return "TaskInformationPOJO{"
                + "taskName='" + taskName + '\''
                + ", numberOfSubtasks=" + numberOfSubtasks
                + ", maxNumberOfSubtasks=" + maxNumberOfSubtasks
                + ", indexOfSubtask=" + indexOfSubtask
                + ", streamConfig=" + streamConfig
                + ", chainedConfig=" + chainedConfig
                + ", localRecoveryConfig=" + localRecoveryConfig
                + '}';
    }
}
