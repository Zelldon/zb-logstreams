/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.tngp.logstreams.log;

import static org.agrona.BitUtil.*;
import static org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor.*;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.agrona.concurrent.UnsafeBuffer;
import org.camunda.tngp.logstreams.spi.LogStorage;

public class MockLogStorage
{
    private final LogStorage mockLogStorage;

    public MockLogStorage()
    {
        this.mockLogStorage = mock(LogStorage.class);

        // default behavior
        when(mockLogStorage.read(any(ByteBuffer.class), anyLong())).thenReturn(LogStorage.OP_RESULT_NO_DATA);
    }

    public MockLogStorage add(MockLogEntryBuilder builder)
    {
        builder.build(mockLogStorage);

        return this;
    }

    public MockLogStorage firstBlockAddress(long address)
    {
        when(mockLogStorage.getFirstBlockAddress()).thenReturn(address);

        return this;
    }

    public LogStorage getMock()
    {
        return mockLogStorage;
    }

    public static MockLogEntryBuilder newLogEntry()
    {
        return new MockLogEntryBuilder(1);
    }

    public static MockLogEntryBuilder newLogEntries(int amount)
    {
        return new MockLogEntryBuilder(amount);
    }

    public static class MockLogEntryBuilder
    {
        private long address = 0;
        private long nextAddress = 0;

        private long position = 0;

        private int sourceEventLogStreamId = 1;
        private long sourceEventPosition = -1L;

        private int producerId = -1;

        private long key = 0;
        private int messageLength = 0;

        private byte[] value = null;

        private byte[] metadata = null;

        private final int amount;

        public MockLogEntryBuilder(int amount)
        {
            this.amount = amount;
        }

        public MockLogEntryBuilder address(long address)
        {
            this.address = address;
            return this;
        }

        public MockLogEntryBuilder position(long position)
        {
            this.position = position;
            return this;
        }

        public MockLogEntryBuilder sourceEventLogStreamId(int logStreamId)
        {
            this.sourceEventLogStreamId = logStreamId;
            return this;
        }

        public MockLogEntryBuilder sourceEventPosition(long position)
        {
            this.sourceEventPosition = position;
            return this;
        }

        public MockLogEntryBuilder producerId(int producerId)
        {
            this.producerId = producerId;
            return this;
        }

        public MockLogEntryBuilder key(long key)
        {
            this.key = key;
            return this;
        }

        public MockLogEntryBuilder value(byte[] value)
        {
            this.value = value;
            return this;
        }

        public MockLogEntryBuilder metadata(byte[] metadata)
        {
            this.metadata = metadata;
            return this;
        }

        public MockLogEntryBuilder messageLength(int messageLength)
        {
            this.messageLength = messageLength;
            return this;
        }

        public MockLogEntryBuilder nextAddress(long nextAddress)
        {
            this.nextAddress = nextAddress;
            return this;
        }

        public void build(LogStorage mockLogStorage)
        {
            final short metadataLength = (short) (metadata != null ? metadata.length : 0);
            final int headerLength = headerLength(SIZE_OF_LONG, metadataLength);

            // min. message length
            messageLength = Math.max(messageLength, headerLength);

            if (value == null)
            {
                value = new byte[messageLength - headerLength];
                new Random().nextBytes(value);
            }
            else
            {
                messageLength = headerLength + value.length;
            }

            when(mockLogStorage.read(any(ByteBuffer.class), eq(address))).thenAnswer(invocation ->
            {
                final ByteBuffer byteBuffer = (ByteBuffer) invocation.getArguments()[0];
                final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);

                int offset = byteBuffer.position();

                for (int i = 0; i < amount; i++)
                {
                    buffer.putInt(lengthOffset(offset), messageLength);

                    final int messageOffset = messageOffset(offset);
                    if (messageOffset <= byteBuffer.limit())
                    {
                        buffer.putLong(positionOffset(messageOffset), position + i);

                        buffer.putInt(producerIdOffset(messageOffset), producerId);

                        buffer.putInt(sourceEventLogStreamIdOffset(messageOffset), sourceEventLogStreamId);
                        buffer.putLong(sourceEventPositionOffset(messageOffset), sourceEventPosition);

                        buffer.putLong(keyOffset(messageOffset), key + i);
                        buffer.putShort(keyLengthOffset(messageOffset), (short) SIZE_OF_LONG);

                        buffer.putShort(metadataLengthOffset(messageOffset, SIZE_OF_LONG), metadataLength);
                        if (metadata != null)
                        {
                            buffer.putBytes(metadataOffset(messageOffset, SIZE_OF_LONG), metadata);
                        }

                        final int valueOffset = valueOffset(messageOffset, SIZE_OF_LONG, metadataLength);
                        final byte[] valueToWrite = Arrays.copyOf(value, Math.min(value.length, byteBuffer.limit() - valueOffset));
                        buffer.putBytes(valueOffset, valueToWrite);
                    }

                    offset += alignedLength(messageLength);
                }

                byteBuffer.position(Math.min(offset, byteBuffer.limit()));

                return nextAddress;
            });
        }
    }

}