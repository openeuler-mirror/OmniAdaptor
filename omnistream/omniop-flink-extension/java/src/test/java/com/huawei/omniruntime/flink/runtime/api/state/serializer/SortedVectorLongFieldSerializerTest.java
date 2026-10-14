package com.huawei.omniruntime.flink.runtime.api.state.serializer;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.entity.SortedVectorLong;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniSerializerJsonInfo;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SortedVectorLongFieldSerializerTest {
    @Test
    void explicitListFieldSurvivesSerializerSnapshotRoundTrip() {
        Map<String, Object> longInfo = new HashMap<>();
        longInfo.put("type", 3);
        Map<String, Object> listInfo = new HashMap<>();
        listInfo.put("type", 1);
        listInfo.put("valueSerializer", JsonHelper.toJson(longInfo));
        Map<String, Object> fieldInfo = new HashMap<>();
        fieldInfo.put("fieldName", "list");
        fieldInfo.put("fieldSerializer", JsonHelper.toJson(listInfo));
        Map<String, Object> pojoInfo = new HashMap<>();
        pojoInfo.put("type", 7);
        pojoInfo.put("element_type", SortedVectorLong.class.getName());
        pojoInfo.put("fields", JsonHelper.toJson(Collections.singletonList(fieldInfo)));

        StateDescriptor<?, ?> descriptor = OmniStateSerializerHelper.buildStateDescriptor(
                "data-state-with-append", "stateSerializer", JsonHelper.toJson(pojoInfo),
                new ExecutionConfig(), getClass().getClassLoader());
        TypeSerializer<?> serializer = descriptor.getSerializer();
        assertTrue(serializer instanceof PojoSerializer);
        assertListOfLong(serializer);
        TypeSerializer<?> restored = serializer.snapshotConfiguration().restoreSerializer();
        assertListOfLong(restored);

        OmniSerializerJsonInfo json = OmniStateSerializerHelper.buildJsonInfo(restored);
        assertEquals(Arrays.asList("list"), json.getFields());
        assertEquals(ListSerializer.class.getName(), json.getFieldSerializers().get(0).getSerializerName());
        assertEquals(LongSerializer.class.getName(),
                json.getFieldSerializers().get(0).getElementSerializer().getSerializerName());
    }

    private void assertListOfLong(TypeSerializer<?> serializer) {
        TypeSerializer<?>[] fields = ReflectionUtils.retrievePrivateField(serializer, "fieldSerializers");
        assertEquals(1, fields.length);
        assertTrue(fields[0] instanceof ListSerializer);
        assertTrue(((ListSerializer<?>) fields[0]).getElementSerializer() instanceof LongSerializer);
    }
}
