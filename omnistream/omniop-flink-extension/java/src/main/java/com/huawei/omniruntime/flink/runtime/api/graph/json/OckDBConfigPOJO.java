/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.huawei.omniruntime.flink.runtime.api.graph.json;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Objects;

/**
 * OckDB配置POJO，镜像OmniStateStore的OckDBOptions，从Flink ReadableConfig解析后下传C++侧。
 *
 * <p>配置项与 com.huawei.ock.bss.OckDBOptions 一一对应，因OmniAdaptor编译期不依赖
 * OmniStateStore插件，此处用字符串key读取，避免直接引用OckDBOptions类。</p>
 *
 * @version 1.0.0
 * @since 2026/08/05
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OckDBConfigPOJO {
    // ---- checkpoint / transfer 相关 ----
    /** state.backend.ockdb.checkpoint.transfer.thread.num，默认4 */
    private int checkpointTransferThreadNum = 4;

    /** state.backend.ockdb.checkpoint.backup，本地checkpoint备份目录，localRecovery开启时必配 */
    private String backupDirectory = "";

    /** state.backend.ockdb.localdir，OckDB本地数据目录 */
    private String localDirectories = "";

    /** state.backend.ockdb.timer-service.factory，优先队列类型 HEAP/OCKDB */
    private String priorityQueueType = "HEAP";

    // ---- JNI 日志相关 ----
    private String jniLogDirectory = "/usr/local/flink/log/kv.log";
    private long jniLogSizeBytes = MemorySize.parse("20mb").getBytes();
    private int jniLogNum = 20;
    private int jniLogLevel = 2;

    // ---- 内存/水位相关 ----
    /** state.backend.ockdb.jni.slice.watermark.ratio，默认0.8 */
    private float jniSliceWatermarkRatio = 0.8F;

    /** state.backend.ockdb.file.memory.fraction，默认0.2 */
    private float fileMemoryFraction = 0.2F;

    // ---- LSM 相关 ----
    /** state.backend.ockdb.jni.lsmstore.compaction.switch，默认1 */
    private int lsmCompactionSwitch = 1;

    /** state.backend.ockdb.lsmstore.compression.policy，默认lz4 */
    private String lsmCompressionPolicy = "lz4";

    /** state.backend.ockdb.lsmstore.compression.level.policy，默认none,none,lz4 */
    private String lsmCompressionLevelPolicy = "none,none,lz4";

    /** state.backend.ockdb.snapshot.compression.algo，默认none */
    private String snapshotCompressionAlgo = "none";

    // ---- filter 相关 ----
    /** state.backend.ockdb.ttl.filter.switch，默认false */
    private boolean ttlFilterSwitch = false;

    /** state.backend.ockdb.cache.filter.and.index.switch，默认true */
    private boolean cacheFilterAndIndexSwitch = true;

    /** state.backend.ockdb.cache.filter.and.index.ratio，默认0.0 */
    private float filterAndIndexOwnCacheRatio = 0.0F;

    /** state.backend.ockdb.bloom.filter.switch，默认true */
    private boolean bloomFilterSwitch = true;

    /** state.backend.bloom.filter.expected.key.count，默认8000000 */
    private int bloomFilterExpectedKeyCount = 8000000;

    /** state.backend.ockdb.peak.filter.elem.num，默认0 */
    private int peakFilterElemNum = 0;

    // ---- KV分离相关 ----
    /** state.backend.ockdb.kv-separate.switch，默认false */
    private boolean kvSeparateSwitch = false;

    /** state.backend.ockdb.kv-separate.threshold，默认200 */
    private int kvSeparateThreshold = 200;

    // ---- 其它 ----
    /** state.backend.ockdb.lazy.download.switch，默认false */
    private boolean lazyDownSwitch = false;

    public OckDBConfigPOJO() {
    }

    /**
     * 从Flink ReadableConfig解析OckDB配置，配置key与OmniStateStore OckDBOptions一致。
     *
     * @param configuration Flink配置
     */
    public OckDBConfigPOJO(ReadableConfig configuration) {
        this.checkpointTransferThreadNum = configuration.get(
            ConfigOptions.key("state.backend.ockdb.checkpoint.transfer.thread.num")
                .intType().defaultValue(4));
        this.backupDirectory = configuration.get(
            ConfigOptions.key("state.backend.ockdb.checkpoint.backup")
                .stringType().defaultValue(""));
        this.localDirectories = configuration.get(
            ConfigOptions.key("state.backend.ockdb.localdir")
                .stringType().defaultValue(""));
        this.priorityQueueType = configuration.get(
            ConfigOptions.key("state.backend.ockdb.timer-service.factory")
                .stringType().defaultValue("HEAP"));

        this.jniLogDirectory = configuration.get(
            ConfigOptions.key("state.backend.ockdb.jni.logfile")
                .stringType().defaultValue("/usr/local/flink/log/kv.log"));
        this.jniLogSizeBytes = configuration.get(
            ConfigOptions.key("state.backend.ockdb.jni.logsize")
                .memoryType().defaultValue(MemorySize.parse("20mb"))).getBytes();
        this.jniLogNum = configuration.get(
            ConfigOptions.key("state.backend.ockdb.jni.lognum")
                .intType().defaultValue(20));
        this.jniLogLevel = configuration.get(
            ConfigOptions.key("state.backend.ockdb.jni.loglevel")
                .intType().defaultValue(2));

        this.jniSliceWatermarkRatio = configuration.get(
            ConfigOptions.key("state.backend.ockdb.jni.slice.watermark.ratio")
                .floatType().defaultValue(0.8F));
        this.fileMemoryFraction = configuration.get(
            ConfigOptions.key("state.backend.ockdb.file.memory.fraction")
                .floatType().defaultValue(0.2F));

        this.lsmCompactionSwitch = configuration.get(
            ConfigOptions.key("state.backend.ockdb.jni.lsmstore.compaction.switch")
                .intType().defaultValue(1));
        this.lsmCompressionPolicy = configuration.get(
            ConfigOptions.key("state.backend.ockdb.lsmstore.compression.policy")
                .stringType().defaultValue("lz4"));
        this.lsmCompressionLevelPolicy = configuration.get(
            ConfigOptions.key("state.backend.ockdb.lsmstore.compression.level.policy")
                .stringType().defaultValue("none,none,lz4"));
        this.snapshotCompressionAlgo = configuration.get(
            ConfigOptions.key("state.backend.ockdb.snapshot.compression.algo")
                .stringType().defaultValue("none"));

        this.ttlFilterSwitch = configuration.get(
            ConfigOptions.key("state.backend.ockdb.ttl.filter.switch")
                .booleanType().defaultValue(false));
        this.cacheFilterAndIndexSwitch = configuration.get(
            ConfigOptions.key("state.backend.ockdb.cache.filter.and.index.switch")
                .booleanType().defaultValue(true));
        this.filterAndIndexOwnCacheRatio = configuration.get(
            ConfigOptions.key("state.backend.ockdb.cache.filter.and.index.ratio")
                .floatType().defaultValue(0.0F));
        this.bloomFilterSwitch = configuration.get(
            ConfigOptions.key("state.backend.ockdb.bloom.filter.switch")
                .booleanType().defaultValue(true));
        this.bloomFilterExpectedKeyCount = configuration.get(
            ConfigOptions.key("state.backend.bloom.filter.expected.key.count")
                .intType().defaultValue(8000000));
        this.peakFilterElemNum = configuration.get(
            ConfigOptions.key("state.backend.ockdb.peak.filter.elem.num")
                .intType().defaultValue(0));

        this.kvSeparateSwitch = configuration.get(
            ConfigOptions.key("state.backend.ockdb.kv-separate.switch")
                .booleanType().defaultValue(false));
        this.kvSeparateThreshold = configuration.get(
            ConfigOptions.key("state.backend.ockdb.kv-separate.threshold")
                .intType().defaultValue(200));

        this.lazyDownSwitch = configuration.get(
            ConfigOptions.key("state.backend.ockdb.lazy.download.switch")
                .booleanType().defaultValue(false));
    }

    public int getCheckpointTransferThreadNum() {
        return checkpointTransferThreadNum;
    }

    public void setCheckpointTransferThreadNum(int checkpointTransferThreadNum) {
        this.checkpointTransferThreadNum = checkpointTransferThreadNum;
    }

    public String getBackupDirectory() {
        return backupDirectory;
    }

    public void setBackupDirectory(String backupDirectory) {
        this.backupDirectory = backupDirectory;
    }

    public String getLocalDirectories() {
        return localDirectories;
    }

    public void setLocalDirectories(String localDirectories) {
        this.localDirectories = localDirectories;
    }

    public String getPriorityQueueType() {
        return priorityQueueType;
    }

    public void setPriorityQueueType(String priorityQueueType) {
        this.priorityQueueType = priorityQueueType;
    }

    public String getJniLogDirectory() {
        return jniLogDirectory;
    }

    public void setJniLogDirectory(String jniLogDirectory) {
        this.jniLogDirectory = jniLogDirectory;
    }

    public long getJniLogSizeBytes() {
        return jniLogSizeBytes;
    }

    public void setJniLogSizeBytes(long jniLogSizeBytes) {
        this.jniLogSizeBytes = jniLogSizeBytes;
    }

    public int getJniLogNum() {
        return jniLogNum;
    }

    public void setJniLogNum(int jniLogNum) {
        this.jniLogNum = jniLogNum;
    }

    public int getJniLogLevel() {
        return jniLogLevel;
    }

    public void setJniLogLevel(int jniLogLevel) {
        this.jniLogLevel = jniLogLevel;
    }

    public float getJniSliceWatermarkRatio() {
        return jniSliceWatermarkRatio;
    }

    public void setJniSliceWatermarkRatio(float jniSliceWatermarkRatio) {
        this.jniSliceWatermarkRatio = jniSliceWatermarkRatio;
    }

    public float getFileMemoryFraction() {
        return fileMemoryFraction;
    }

    public void setFileMemoryFraction(float fileMemoryFraction) {
        this.fileMemoryFraction = fileMemoryFraction;
    }

    public int getLsmCompactionSwitch() {
        return lsmCompactionSwitch;
    }

    public void setLsmCompactionSwitch(int lsmCompactionSwitch) {
        this.lsmCompactionSwitch = lsmCompactionSwitch;
    }

    public String getLsmCompressionPolicy() {
        return lsmCompressionPolicy;
    }

    public void setLsmCompressionPolicy(String lsmCompressionPolicy) {
        this.lsmCompressionPolicy = lsmCompressionPolicy;
    }

    public String getLsmCompressionLevelPolicy() {
        return lsmCompressionLevelPolicy;
    }

    public void setLsmCompressionLevelPolicy(String lsmCompressionLevelPolicy) {
        this.lsmCompressionLevelPolicy = lsmCompressionLevelPolicy;
    }

    public String getSnapshotCompressionAlgo() {
        return snapshotCompressionAlgo;
    }

    public void setSnapshotCompressionAlgo(String snapshotCompressionAlgo) {
        this.snapshotCompressionAlgo = snapshotCompressionAlgo;
    }

    public boolean getTtlFilterSwitch() {
        return ttlFilterSwitch;
    }

    public void setTtlFilterSwitch(boolean ttlFilterSwitch) {
        this.ttlFilterSwitch = ttlFilterSwitch;
    }

    public boolean getCacheFilterAndIndexSwitch() {
        return cacheFilterAndIndexSwitch;
    }

    public void setCacheFilterAndIndexSwitch(boolean cacheFilterAndIndexSwitch) {
        this.cacheFilterAndIndexSwitch = cacheFilterAndIndexSwitch;
    }

    public float getFilterAndIndexOwnCacheRatio() {
        return filterAndIndexOwnCacheRatio;
    }

    public void setFilterAndIndexOwnCacheRatio(float filterAndIndexOwnCacheRatio) {
        this.filterAndIndexOwnCacheRatio = filterAndIndexOwnCacheRatio;
    }

    public boolean getBloomFilterSwitch() {
        return bloomFilterSwitch;
    }

    public void setBloomFilterSwitch(boolean bloomFilterSwitch) {
        this.bloomFilterSwitch = bloomFilterSwitch;
    }

    public int getBloomFilterExpectedKeyCount() {
        return bloomFilterExpectedKeyCount;
    }

    public void setBloomFilterExpectedKeyCount(int bloomFilterExpectedKeyCount) {
        this.bloomFilterExpectedKeyCount = bloomFilterExpectedKeyCount;
    }

    public int getPeakFilterElemNum() {
        return peakFilterElemNum;
    }

    public void setPeakFilterElemNum(int peakFilterElemNum) {
        this.peakFilterElemNum = peakFilterElemNum;
    }

    public boolean getKvSeparateSwitch() {
        return kvSeparateSwitch;
    }

    public void setKvSeparateSwitch(boolean kvSeparateSwitch) {
        this.kvSeparateSwitch = kvSeparateSwitch;
    }

    public int getKvSeparateThreshold() {
        return kvSeparateThreshold;
    }

    public void setKvSeparateThreshold(int kvSeparateThreshold) {
        this.kvSeparateThreshold = kvSeparateThreshold;
    }

    public boolean getLazyDownSwitch() {
        return lazyDownSwitch;
    }

    public void setLazyDownSwitch(boolean lazyDownSwitch) {
        this.lazyDownSwitch = lazyDownSwitch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OckDBConfigPOJO that = (OckDBConfigPOJO) o;
        return checkpointTransferThreadNum == that.checkpointTransferThreadNum
                && jniLogSizeBytes == that.jniLogSizeBytes
                && jniLogNum == that.jniLogNum
                && jniLogLevel == that.jniLogLevel
                && Float.compare(jniSliceWatermarkRatio, that.jniSliceWatermarkRatio) == 0
                && Float.compare(fileMemoryFraction, that.fileMemoryFraction) == 0
                && lsmCompactionSwitch == that.lsmCompactionSwitch
                && ttlFilterSwitch == that.ttlFilterSwitch
                && cacheFilterAndIndexSwitch == that.cacheFilterAndIndexSwitch
                && Float.compare(filterAndIndexOwnCacheRatio, that.filterAndIndexOwnCacheRatio) == 0
                && bloomFilterSwitch == that.bloomFilterSwitch
                && bloomFilterExpectedKeyCount == that.bloomFilterExpectedKeyCount
                && peakFilterElemNum == that.peakFilterElemNum
                && kvSeparateSwitch == that.kvSeparateSwitch
                && kvSeparateThreshold == that.kvSeparateThreshold
                && lazyDownSwitch == that.lazyDownSwitch
                && Objects.equals(backupDirectory, that.backupDirectory)
                && Objects.equals(localDirectories, that.localDirectories)
                && Objects.equals(priorityQueueType, that.priorityQueueType)
                && Objects.equals(jniLogDirectory, that.jniLogDirectory)
                && Objects.equals(lsmCompressionPolicy, that.lsmCompressionPolicy)
                && Objects.equals(lsmCompressionLevelPolicy, that.lsmCompressionLevelPolicy)
                && Objects.equals(snapshotCompressionAlgo, that.snapshotCompressionAlgo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                checkpointTransferThreadNum,
                backupDirectory,
                localDirectories,
                priorityQueueType,
                jniLogDirectory,
                jniLogSizeBytes,
                jniLogNum,
                jniLogLevel,
                jniSliceWatermarkRatio,
                fileMemoryFraction,
                lsmCompactionSwitch,
                lsmCompressionPolicy,
                lsmCompressionLevelPolicy,
                snapshotCompressionAlgo,
                ttlFilterSwitch,
                cacheFilterAndIndexSwitch,
                filterAndIndexOwnCacheRatio,
                bloomFilterSwitch,
                bloomFilterExpectedKeyCount,
                peakFilterElemNum,
                kvSeparateSwitch,
                kvSeparateThreshold,
                lazyDownSwitch);
    }
}
