/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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

package com.huawei.omniruntime.flink.runtime.metrics;

import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

/**
 * OmniDescriptiveStatisticsHistogram is a specialized histogram that provides descriptive statistics
 * for a set of values. It extends the DescriptiveStatisticsHistogram class and implements the
 * MetricCloseable interface.
 * <p>
 * This class is used to collect and analyze statistical data, such as mean, standard deviation,
 * minimum, maximum, and quantiles.
 *
 * @since 2025-04-16
 */
public class OmniDescriptiveStatisticsHistogram extends DescriptiveStatisticsHistogram implements MetricCloseable {
    private long nativeRef = 0L;

    private volatile boolean isClosed = false;
    private volatile long originalCount = 0L;
    private volatile String originalStatistics;

    public OmniDescriptiveStatisticsHistogram(long nativeRef, int windowSize) {
        super(windowSize);
        this.nativeRef = nativeRef;
    }

    /**
     * get count.
     *
     * @return count
     */
    public long getCount() {
        // jni call to get the count
        if (isClosed) {
            return originalCount;
        } else {
            originalCount = getNativeCount(nativeRef);
        }
        return originalCount;
    }

    /**
     * get statistics.
     *
     * @return statistics
     */
    public HistogramStatistics getStatistics() {
        // jni call to get the statistics
        if (!isClosed) {
            originalStatistics = getStatistics(nativeRef);
        }
        // future do convert statistics to HistogramStatistics
        return new HistogramStatistics() {
            @Override
            public double getQuantile(double v) {
                return 0;
            }

            @Override
            public long[] getValues() {
                return new long[0];
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public double getMean() {
                return 0;
            }

            @Override
            public double getStdDev() {
                return 0;
            }

            @Override
            public long getMax() {
                return 0;
            }

            @Override
            public long getMin() {
                return 0;
            }
        };
    }

    /**
     * Close the histogram and release any resources associated with it.
     */
    public void close() {
        isClosed = true;
    }

    /**
     * Get the count of the histogram from JNI.
     *
     * @param nativeRef The native reference to the histogram.
     * @return The count of the histogram.
     */
    public native long getNativeCount(long nativeRef);

    /**
     * Get the statistics of the histogram from JNI.
     *
     * @param nativeRef The native reference to the histogram.
     * @return The statistics of the histogram.
     */
    public native String getStatistics(long nativeRef);
}
