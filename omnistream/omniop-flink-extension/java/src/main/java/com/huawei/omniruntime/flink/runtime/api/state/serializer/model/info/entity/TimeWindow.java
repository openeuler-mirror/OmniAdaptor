package com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.entity;

public class TimeWindow {
    long start;
    long end;

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }
}
