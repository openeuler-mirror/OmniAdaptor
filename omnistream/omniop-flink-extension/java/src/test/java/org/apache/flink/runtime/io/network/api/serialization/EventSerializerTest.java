package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests the OmniStream extension of Flink checkpoint-barrier serialization. */
class EventSerializerTest {

    /** Offset of the savepoint format code in a serialized checkpoint barrier. */
    private static final int SAVEPOINT_FORMAT_CODE_OFFSET =
            Integer.BYTES + Long.BYTES + Long.BYTES + Byte.BYTES;

    /** Wire-format code shared by the Java and C++ COMPATIBLE serializers. */
    private static final byte SAVEPOINT_FORMAT_COMPATIBLE = 2;

    /**
     * Verifies that every COMPATIBLE savepoint action retains code 2 and its original semantics.
     */
    @Test
    void shouldRoundTripCompatibleSavepointTypes() throws Exception {
        SavepointType[] savepointTypes = {
            SavepointType.savepoint(SavepointFormatType.COMPATIBLE),
            SavepointType.suspend(SavepointFormatType.COMPATIBLE),
            SavepointType.terminate(SavepointFormatType.COMPATIBLE)
        };

        for (SavepointType savepointType : savepointTypes) {
            CheckpointBarrier barrier =
                    new CheckpointBarrier(
                            1L,
                            2L,
                            new CheckpointOptions(
                                    savepointType,
                                    CheckpointStorageLocationReference.getDefault()));

            ByteBuffer serialized = EventSerializer.toSerializedEvent(barrier);

            assertEquals(SAVEPOINT_FORMAT_COMPATIBLE, serialized.get(SAVEPOINT_FORMAT_CODE_OFFSET));
            assertEquals(
                    barrier,
                    EventSerializer.fromSerializedEvent(
                            serialized, EventSerializerTest.class.getClassLoader()));
        }
    }
}
