package com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.entity;

import java.util.List;

public class VectorBatch {
    int flatSize = 0;
    long capacity;
    long rowCnt;
    List<Object> vectors;

    public int getFlatSize() {
        return flatSize;
    }

    public void setFlatSize(int flatSize) {
        this.flatSize = flatSize;
    }

    public long getCapacity() {
        return capacity;
    }

    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    public long getRowCnt() {
        return rowCnt;
    }

    public void setRowCnt(long rowCnt) {
        this.rowCnt = rowCnt;
    }

    public List<Object> getVectors() {
        return vectors;
    }

    public void setVectors(List<Object> vectors) {
        this.vectors = vectors;
    }
}
