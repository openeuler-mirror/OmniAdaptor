package com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.type;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.util.Preconditions;

import java.util.List;

public class BinaryTypeInfo<T> extends TypeInformation<T> {

    private T t;
    private TypeSerializer<T> typeSerializer;

    public BinaryTypeInfo(T t, TypeSerializer<T> typeSerializer) {
        this.t = Preconditions.checkNotNull(t, "The T obj cannot be null.");
        this.typeSerializer = Preconditions.checkNotNull(typeSerializer, "The typeSerializer cannot be null.");
    }

    public static BinaryTypeInfo<BinaryRowData> of(BinaryRowData binaryRowData, List<String> inputTypes) {
        return new BinaryTypeInfo<>(binaryRowData, new BinaryRowDataSerializer(binaryRowData.getArity(), inputTypes));
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public int getTotalFields() {
        return 0;
    }

    @Override
    public Class<T> getTypeClass() {
        return (Class<T>) t.getClass();
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return typeSerializer;
    }

    @Override
    public String toString() {
        return t.getClass().getSimpleName();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (!(obj instanceof BinaryTypeInfo)) {
            return false;
        } else {
            BinaryTypeInfo<T> other = (BinaryTypeInfo) obj;
            return other.canEqual(this) && this.t.equals(other.t);
        }
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj != null && obj.getClass() == this.getClass();
    }
}
