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

import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor.alignedLength;
import static org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor.messageOffset;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.KEY_TYPE_UINT64;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.keyLengthOffset;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.keyOffset;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.keyTypeOffset;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.metadataOffset;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.positionOffset;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.producerIdOffset;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.sourceEventLogStreamIdOffset;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.sourceEventPositionOffset;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.valueOffset;
import static org.camunda.tngp.util.StringUtil.getBytes;
import static org.camunda.tngp.util.buffer.BufferUtil.wrapString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.camunda.tngp.dispatcher.ClaimedFragment;
import org.camunda.tngp.dispatcher.Dispatcher;
import org.camunda.tngp.dispatcher.impl.log.LogBufferAppender;
import org.camunda.tngp.util.buffer.BufferWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

public class LogStreamWriterTest
{
    private static final DirectBuffer TOPIC_NAME = wrapString("test-topic");
    private static final int PARTITION_ID = 1;
    private static final byte[] EVENT_VALUE = getBytes("test");
    private static final byte[] EVENT_METADATA = getBytes("metadata");

    private static final int MESSAGE_OFFSET = messageOffset(0);

    @Mock
    private LogStream mockLog;

    @Mock
    private Dispatcher mockWriteBuffer;

    @Mock
    private BufferWriter mockBufferWriter;

    @Mock
    private BufferWriter mockMetadataWriter;

    private UnsafeBuffer writeBuffer;

    private LogStreamWriter writer;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void init() throws Exception
    {
        MockitoAnnotations.initMocks(this);

        when(mockLog.getWriteBuffer()).thenReturn(mockWriteBuffer);
        when(mockLog.getTopicName()).thenReturn(TOPIC_NAME);
        when(mockLog.getPartitionId()).thenReturn(PARTITION_ID);

        writer = new LogStreamWriterImpl(mockLog);

        writeBuffer = new UnsafeBuffer(new byte[1024]);
    }

    @Test
    public void shouldWriteEvent()
    {
        final long dispatcherPosition = 24L;
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), eq(PARTITION_ID))).thenAnswer(claimFragment(dispatcherPosition));

        final long position = writer
            .key(4L)
            .value(new UnsafeBuffer(EVENT_VALUE))
            .tryWrite();

        assertThat(position).isEqualTo(dispatcherPosition);
        assertThat(writeBuffer.getLong(positionOffset(MESSAGE_OFFSET))).isEqualTo(position);

        final byte[] valueBuffer = new byte[EVENT_VALUE.length];
        writeBuffer.getBytes(valueOffset(MESSAGE_OFFSET, SIZE_OF_LONG, 0), valueBuffer);
        assertThat(valueBuffer).isEqualTo(EVENT_VALUE);
    }

    @Test
    public void shouldWriteEventWithValueBuffer()
    {
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), anyInt())).thenAnswer(claimFragment(24));

        writer
            .key(4L)
            .value(new UnsafeBuffer(EVENT_VALUE), 1, 2)
            .tryWrite();

        final byte[] valueBuffer = new byte[2];
        writeBuffer.getBytes(valueOffset(MESSAGE_OFFSET, SIZE_OF_LONG, 0), valueBuffer);
        assertThat(valueBuffer).isEqualTo(new byte[] {EVENT_VALUE[1], EVENT_VALUE[2]});
    }

    @Test
    public void shouldWriteEventWithValueWriter()
    {
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), anyInt())).thenAnswer(claimFragment(24));

        writer
            .key(4L)
            .valueWriter(mockBufferWriter)
            .tryWrite();

        final int valueOffset = valueOffset(MESSAGE_OFFSET, SIZE_OF_LONG, 0);
        verify(mockBufferWriter).write(any(), eq(valueOffset));
    }

    @Test
    public void shouldWriteEventWithMetadataBuffer()
    {
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), anyInt())).thenAnswer(claimFragment(24));

        writer
            .key(4L)
            .metadata(new UnsafeBuffer(EVENT_METADATA), 3, 4)
            .valueWriter(mockBufferWriter)
            .tryWrite();

        final byte[] valueBuffer = new byte[2];
        writeBuffer.getBytes(metadataOffset(MESSAGE_OFFSET, SIZE_OF_LONG), valueBuffer);
        assertThat(valueBuffer).isEqualTo(new byte[] {EVENT_METADATA[3], EVENT_METADATA[4]});
    }

    @Test
    public void shouldWriteEventWithMetadataWriter()
    {
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), anyInt())).thenAnswer(claimFragment(24));
        when(mockMetadataWriter.getLength()).thenReturn(EVENT_METADATA.length);

        writer
            .key(4L)
            .metadataWriter(mockMetadataWriter)
            .valueWriter(mockBufferWriter)
            .tryWrite();

        final int valueOffset = metadataOffset(MESSAGE_OFFSET, SIZE_OF_LONG);
        verify(mockMetadataWriter).write(any(), eq(valueOffset));
    }

    @Test
    public void shouldWriteEventWithKey()
    {
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), anyInt())).thenAnswer(claimFragment(0));

        writer
            .key(4L)
            .value(new UnsafeBuffer(EVENT_VALUE))
            .tryWrite();

        assertThat(writeBuffer.getShort(keyTypeOffset(MESSAGE_OFFSET))).isEqualTo(KEY_TYPE_UINT64);
        assertThat(writeBuffer.getShort(keyLengthOffset(MESSAGE_OFFSET))).isEqualTo((short) SIZE_OF_LONG);
        assertThat(writeBuffer.getLong(keyOffset(MESSAGE_OFFSET))).isEqualTo(4L);
    }

    @Test
    public void shouldWriteEventWithPositionAsKey()
    {
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), anyInt())).thenAnswer(claimFragment(24));

        final long position = writer
            .positionAsKey()
            .value(new UnsafeBuffer(EVENT_VALUE))
            .tryWrite();

        assertThat(writeBuffer.getShort(keyTypeOffset(MESSAGE_OFFSET))).isEqualTo(KEY_TYPE_UINT64);
        assertThat(writeBuffer.getShort(keyLengthOffset(MESSAGE_OFFSET))).isEqualTo((short) SIZE_OF_LONG);
        assertThat(writeBuffer.getLong(keyOffset(MESSAGE_OFFSET))).isEqualTo(position);
    }

    @Test
    public void shouldWriteEventWithSourceEvent()
    {
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), anyInt())).thenAnswer(claimFragment(0));

        writer
            .positionAsKey()
            .value(new UnsafeBuffer(EVENT_VALUE))
            .sourceEvent(TOPIC_NAME, PARTITION_ID, 3L)
            .tryWrite();

        assertThat(writeBuffer.getInt(sourceEventLogStreamIdOffset(MESSAGE_OFFSET))).isEqualTo(PARTITION_ID);
        assertThat(writeBuffer.getLong(sourceEventPositionOffset(MESSAGE_OFFSET))).isEqualTo(3L);
    }

    @Test
    public void shouldWriteEventWithoutSourceEvent()
    {
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), anyInt())).thenAnswer(claimFragment(0));

        writer
            .positionAsKey()
            .value(new UnsafeBuffer(EVENT_VALUE))
            .tryWrite();

        assertThat(writeBuffer.getInt(sourceEventLogStreamIdOffset(MESSAGE_OFFSET))).isEqualTo(-1);
        assertThat(writeBuffer.getLong(sourceEventPositionOffset(MESSAGE_OFFSET))).isEqualTo(-1L);
    }

    @Test
    public void shouldWriteEventWithStreamProcessorId()
    {
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), anyInt())).thenAnswer(claimFragment(0));

        writer
            .positionAsKey()
            .producerId(2)
            .value(new UnsafeBuffer(EVENT_VALUE))
            .tryWrite();

        assertThat(writeBuffer.getInt(producerIdOffset(MESSAGE_OFFSET))).isEqualTo(2);
    }

    @Test
    public void shouldRetryIfFailToClaimFragmentOnPaddingAtPartionEnd()
    {
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), anyInt()))
            .thenReturn((long) LogBufferAppender.RESULT_PADDING_AT_END_OF_PARTITION)
            .thenAnswer(claimFragment(24));

        final long position = writer
            .key(4L)
            .value(new UnsafeBuffer(EVENT_VALUE))
            .tryWrite();

        assertThat(position).isEqualTo(24);
    }

    @Test
    public void shouldNotRetryIfFailToClaimFragmentOnPartionEnd()
    {
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), anyInt()))
            .thenReturn((long) LogBufferAppender.RESULT_END_OF_PARTITION)
            .thenAnswer(claimFragment(24));

        final long position = writer
            .key(4L)
            .value(new UnsafeBuffer(EVENT_VALUE))
            .tryWrite();

        assertThat(position).isEqualTo(-1);
    }

    @Test
    public void shouldFailToWriteEventWithoutValue()
    {
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), anyInt())).thenAnswer(claimFragment(24));

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("value must not be null");

        writer
            .key(4L)
            .tryWrite();
    }

    @Test
    public void shouldFailToWriteEventWithoutKey()
    {
        when(mockWriteBuffer.claim(any(ClaimedFragment.class), anyInt(), anyInt())).thenAnswer(claimFragment(24));

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("key must be greater than or equal to 0");

        writer
            .value(new UnsafeBuffer(EVENT_VALUE))
            .tryWrite();
    }

    protected Answer<?> claimFragment(final long offset)
    {
        return invocation ->
        {
            final ClaimedFragment claimedFragment = (ClaimedFragment) invocation.getArguments()[0];
            final int length = (int) invocation.getArguments()[1];

            claimedFragment.wrap(writeBuffer, 0, alignedLength(length));

            final long claimedPosition = offset + alignedLength(length);
            return claimedPosition;
        };
    }

}
