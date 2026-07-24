/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * V4 serializer that adds {@link org.apache.flink.runtime.checkpoint.CheckpointProperties}
 * serialization.
 */
@Internal
public class MetadataV4Serializer implements MetadataSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataV4Serializer.class);

    public static final MetadataSerializer INSTANCE = new MetadataV4Serializer();
    public static final int VERSION = 4;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public CheckpointMetadata deserialize(
            DataInputStream dis, ClassLoader userCodeClassLoader, String externalPointer)
            throws IOException {
        return MetadataV3Serializer.INSTANCE
                .deserialize(dis, userCodeClassLoader, externalPointer)
                .withProperties(deserializeProperties(dis));
    }

    @Override
    public void serialize(CheckpointMetadata checkpointMetadata, DataOutputStream dos)
            throws IOException {
        MetadataV3Serializer.INSTANCE.serialize(checkpointMetadata, dos);
        serializeProperties(checkpointMetadata.getCheckpointProperties(), dos);
    }

    private CheckpointProperties deserializeProperties(DataInputStream dis) throws IOException {
        try {
            // closed outside
            return (CheckpointProperties) new ObjectInputStream(dis).readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Couldn't deserialize checkpoint properties", e);
        }
    }

    private static void serializeProperties(CheckpointProperties properties, DataOutputStream dos)
            throws IOException {
        CheckpointProperties propertiesToSerialize = properties;
        if (properties.isSavepoint()) {
            SavepointType savepointType = (SavepointType) properties.getCheckpointType();
            SavepointFormatType formatType = savepointType.getFormatType();
            LOG.debug("MetadataV4Serializer.serializeProperties: savepoint formatType={}, isSynchronous={}",
                    formatType, properties.isSynchronous());
            if (formatType == SavepointFormatType.COMPATIBLE) {
                LOG.debug("MetadataV4Serializer.serializeProperties: converting COMPATIBLE to CANONICAL format");
                SavepointType newSavepointType = createCanonicalSavepointType(savepointType);
                propertiesToSerialize = copyWithNewCheckpointType(properties, newSavepointType);
                LOG.debug("MetadataV4Serializer.serializeProperties: created a copy with CANONICAL format, new formatType={}",
                        newSavepointType.getFormatType());
            }
        }
        new ObjectOutputStream(dos).writeObject(propertiesToSerialize); // closed outside
    }

    /**
     * Constructor parameter names of CheckpointProperties, in the exact order the constructor
     * expects them. We must not rely on field declaration order because the constructor parameter
     * order is different (forced comes first, while field declaration has checkpointType first).
     */
    private static final String[] CONSTRUCTOR_PARAM_NAMES = {
        "forced", "checkpointType", "discardSubsumed", "discardFinished",
        "discardCancelled", "discardFailed", "discardSuspended", "unclaimed"
    };

    /**
     * Create a deep copy of the given CheckpointProperties, replacing its checkpointType with the
     * given newType. The original properties object is not modified.
     */
    private static CheckpointProperties copyWithNewCheckpointType(
            CheckpointProperties original, SavepointType newType) throws IOException {
        try {
            Object[] fieldValues = new Object[CONSTRUCTOR_PARAM_NAMES.length];
            Class<?>[] paramTypes = new Class<?>[CONSTRUCTOR_PARAM_NAMES.length];

            for (int i = 0; i < CONSTRUCTOR_PARAM_NAMES.length; i++) {
                Field field = CheckpointProperties.class.getDeclaredField(CONSTRUCTOR_PARAM_NAMES[i]);
                field.setAccessible(true);
                paramTypes[i] = field.getType();
                if ("checkpointType".equals(CONSTRUCTOR_PARAM_NAMES[i])) {
                    fieldValues[i] = newType;
                } else {
                    fieldValues[i] = field.get(original);
                }
            }

            Constructor<?> constructor = CheckpointProperties.class.getDeclaredConstructor(paramTypes);
            constructor.setAccessible(true);
            CheckpointProperties copy = (CheckpointProperties) constructor.newInstance(fieldValues);
            LOG.debug("MetadataV4Serializer.copyWithNewCheckpointType: created a deep copy of CheckpointProperties with new checkpointType");
            return copy;
        } catch (Exception e) {
            LOG.debug("MetadataV4Serializer.copyWithNewCheckpointType: failed to create copy", e);
            throw new IOException("Failed to create a copy of CheckpointProperties", e);
        }
    }

    private static SavepointType createCanonicalSavepointType(SavepointType original) throws IOException {
        try {
            Method factoryMethod = SavepointType.class.getDeclaredMethod(
                    "savepoint", SavepointFormatType.class);
            SavepointType newType = (SavepointType) factoryMethod.invoke(null, SavepointFormatType.CANONICAL);

            LOG.debug("MetadataV4Serializer.createCanonicalSavepointType: created new savepoint type with CANONICAL format");
            return newType;
        } catch (Exception e) {
            LOG.warn("MetadataV4Serializer.createCanonicalSavepointType: failed to use factory method, trying reflection approach", e);
            return createCanonicalSavepointTypeViaReflection(original);
        }
    }

    private static SavepointType createCanonicalSavepointTypeViaReflection(SavepointType original) throws IOException {
        try {
            Field postActionField = SavepointType.class.getDeclaredField("postCheckpointAction");
            postActionField.setAccessible(true);
            Object postCheckpointAction = postActionField.get(original);

            Constructor<?> constructor = SavepointType.class.getDeclaredConstructor(
                    SavepointType.PostCheckpointAction.class, SavepointFormatType.class);
            constructor.setAccessible(true);

            SavepointType newType = (SavepointType) constructor.newInstance(
                    postCheckpointAction, SavepointFormatType.CANONICAL);
            LOG.debug("MetadataV4Serializer.createCanonicalSavepointTypeViaReflection: created new savepoint type with CANONICAL format");
            return newType;
        } catch (Exception e) {
            LOG.error("MetadataV4Serializer.createCanonicalSavepointTypeViaReflection: failed to create new savepoint type", e);
            throw new IOException("Failed to create new SavepointType with CANONICAL format", e);
        }
    }

}
