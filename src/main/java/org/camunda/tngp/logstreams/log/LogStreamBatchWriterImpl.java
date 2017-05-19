package org.camunda.tngp.logstreams.log;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor.alignedLength;
import static org.camunda.tngp.dispatcher.impl.log.LogBufferAppender.RESULT_PADDING_AT_END_OF_PARTITION;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.HEADER_BLOCK_LENGTH;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.headerLength;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.metadataOffset;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.setKey;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.setMetadataLength;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.setPosition;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.setProducerId;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.setSourceEventLogStreamPartitionId;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.setSourceEventLogStreamTopicNameLength;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.setSourceEventPosition;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.sourceEventLogStreamTopicNameOffset;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.valueOffset;
import static org.camunda.tngp.util.EnsureUtil.ensureGreaterThan;
import static org.camunda.tngp.util.EnsureUtil.ensureGreaterThanOrEqual;
import static org.camunda.tngp.util.EnsureUtil.ensureNotNull;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.camunda.tngp.dispatcher.ClaimedFragmentBatch;
import org.camunda.tngp.dispatcher.Dispatcher;
import org.camunda.tngp.logstreams.log.LogStreamBatchWriter.LogEntryBuilder;
import org.camunda.tngp.util.buffer.BufferWriter;
import org.camunda.tngp.util.buffer.DirectBufferWriter;

public class LogStreamBatchWriterImpl implements LogStreamBatchWriter, LogEntryBuilder
{
    private static final int INITIAL_BUFFER_CAPACITY = 1024 * 32;

    private static final long POSITION_AS_KEY = -1L;

    private final ClaimedFragmentBatch claimedBatch = new ClaimedFragmentBatch();

    private MutableDirectBuffer eventBuffer = new ExpandableArrayBuffer(INITIAL_BUFFER_CAPACITY);

    private final DirectBufferWriter metadataWriterInstance = new DirectBufferWriter();
    private final DirectBufferWriter bufferWriterInstance = new DirectBufferWriter();

    private int eventBufferOffset;
    private int eventLength;
    private int eventCount;

    private Dispatcher logWriteBuffer;
    private int logId;

    private boolean positionAsKey;
    private long key;

    private int producerId;

    private long sourceEventPosition;
    private final DirectBuffer sourceEventLogStreamTopicName = new UnsafeBuffer(0, 0);
    private int sourceEventLogStreamPartitionId;

    private BufferWriter metadataWriter;
    private BufferWriter valueWriter;

    public LogStreamBatchWriterImpl()
    {
        reset();
    }

    public LogStreamBatchWriterImpl(LogStream log)
    {
        wrap(log);
    }

    @Override
    public void wrap(LogStream log)
    {
        this.logWriteBuffer = log.getWriteBuffer();
        this.logId = log.getPartitionId();

        reset();
    }

    @Override
    public LogStreamBatchWriter sourceEvent(final DirectBuffer logStreamTopicName, int logStreamPartitionId, long position)
    {
        this.sourceEventLogStreamTopicName.wrap(logStreamTopicName);
        this.sourceEventLogStreamPartitionId = logStreamPartitionId;
        this.sourceEventPosition = position;
        return this;
    }

    @Override
    public LogStreamBatchWriter producerId(int producerId)
    {
        this.producerId = producerId;
        return this;
    }

    @Override
    public LogEntryBuilder event()
    {
        resetEvent();
        return this;
    }

    @Override
    public LogEntryBuilder positionAsKey()
    {
        positionAsKey = true;
        return this;
    }

    @Override
    public LogEntryBuilder key(long key)
    {
        this.key = key;
        return this;
    }

    @Override
    public LogEntryBuilder metadata(DirectBuffer buffer, int offset, int length)
    {
        metadataWriterInstance.wrap(buffer, offset, length);
        return this;
    }

    @Override
    public LogEntryBuilder metadata(DirectBuffer buffer)
    {
        return metadata(buffer, 0, buffer.capacity());
    }

    @Override
    public LogEntryBuilder metadataWriter(BufferWriter writer)
    {
        this.metadataWriter = writer;
        return this;
    }

    @Override
    public LogEntryBuilder value(DirectBuffer value, int valueOffset, int valueLength)
    {
        return valueWriter(bufferWriterInstance.wrap(value, valueOffset, valueLength));
    }

    @Override
    public LogEntryBuilder value(DirectBuffer value)
    {
        return value(value, 0, value.capacity());
    }

    @Override
    public LogEntryBuilder valueWriter(BufferWriter writer)
    {
        this.valueWriter = writer;
        return this;
    }

    @Override
    public LogStreamBatchWriter done()
    {
        // validation
        ensureNotNull("value", valueWriter);

        if (!positionAsKey)
        {
            ensureGreaterThanOrEqual("key", key, 0);
        }

        // copy event to buffer
        final int metadataLength = metadataWriter.getLength();
        final int valueLength = valueWriter.getLength();

        eventBuffer.putLong(eventBufferOffset, positionAsKey ? POSITION_AS_KEY : key);
        eventBufferOffset += SIZE_OF_LONG;

        eventBuffer.putInt(eventBufferOffset, metadataLength);
        eventBufferOffset += SIZE_OF_INT;

        eventBuffer.putInt(eventBufferOffset, valueLength);
        eventBufferOffset += SIZE_OF_INT;

        if (metadataLength > 0)
        {
            metadataWriter.write(eventBuffer, eventBufferOffset);
            eventBufferOffset += metadataLength;
        }

        valueWriter.write(eventBuffer, eventBufferOffset);
        eventBufferOffset += valueLength;

        eventLength += metadataLength + valueLength;
        eventCount += 1;

        return this;
    }

    @Override
    public long tryWrite()
    {
        ensureGreaterThan("event count", eventCount, 0);

        long result = claimBatchForEvents();
        if (result >= 0)
        {
            try
            {
                // return position of last event
                result = writeEventsToBuffer(claimedBatch.getBuffer());

                claimedBatch.commit();
            }
            catch (Exception e)
            {
                claimedBatch.abort();
                LangUtil.rethrowUnchecked(e);
            }
            finally
            {
                reset();
            }
        }
        return result;
    }

    private long claimBatchForEvents()
    {
        final int topicNameLength = sourceEventLogStreamTopicName.capacity();
        final int batchLength = eventLength + eventCount * (HEADER_BLOCK_LENGTH + topicNameLength);

        long claimedPosition = -1;
        do
        {
            claimedPosition = logWriteBuffer.claim(claimedBatch, eventCount, batchLength);
        }
        while (claimedPosition == RESULT_PADDING_AT_END_OF_PARTITION);

        return claimedPosition;
    }

    private long writeEventsToBuffer(final MutableDirectBuffer writeBuffer)
    {
        long lastEventPosition = -1L;
        eventBufferOffset = 0;

        for (int i = 0; i < eventCount; i++)
        {
            final long key = eventBuffer.getLong(eventBufferOffset);
            eventBufferOffset += SIZE_OF_LONG;

            final int metadataLength = eventBuffer.getInt(eventBufferOffset);
            eventBufferOffset += SIZE_OF_INT;

            final int valueLength = eventBuffer.getInt(eventBufferOffset);
            eventBufferOffset += SIZE_OF_INT;

            final int topicNameLength = sourceEventLogStreamTopicName.capacity();
            final int fragmentLength = headerLength(topicNameLength, metadataLength) + valueLength;

            // allocate fragment for log entry
            final long nextFragmentPosition = claimedBatch.nextFragment(fragmentLength, logId);
            final int bufferOffset = claimedBatch.getFragmentOffset();

            final long position = nextFragmentPosition - alignedLength(fragmentLength);
            final long keyToWrite = key == POSITION_AS_KEY ? position : key;

            // write log entry header
            setPosition(writeBuffer, bufferOffset, position);
            setProducerId(writeBuffer, bufferOffset, producerId);
            setSourceEventLogStreamPartitionId(writeBuffer, bufferOffset, sourceEventLogStreamPartitionId);
            setSourceEventPosition(writeBuffer, bufferOffset, sourceEventPosition);
            setKey(writeBuffer, bufferOffset, keyToWrite);
            setSourceEventLogStreamTopicNameLength(writeBuffer, bufferOffset, (short) topicNameLength);
            setMetadataLength(writeBuffer, bufferOffset, (short) metadataLength);

            if (topicNameLength > 0)
            {
                writeBuffer.putBytes(sourceEventLogStreamTopicNameOffset(bufferOffset), sourceEventLogStreamTopicName, 0, topicNameLength);
            }

            if (metadataLength > 0)
            {
                writeBuffer.putBytes(metadataOffset(bufferOffset, topicNameLength), eventBuffer, eventBufferOffset, metadataLength);
                eventBufferOffset += metadataLength;
            }

            // write log entry value
            writeBuffer.putBytes(valueOffset(bufferOffset, topicNameLength, metadataLength), eventBuffer, eventBufferOffset, valueLength);
            eventBufferOffset += valueLength;

            lastEventPosition = position;
        }
        return lastEventPosition;
    }

    @Override
    public void reset()
    {
        eventBufferOffset = 0;
        eventLength = 0;
        eventCount = 0;

        sourceEventLogStreamTopicName.wrap(0, 0);
        sourceEventLogStreamPartitionId = -1;
        sourceEventPosition = -1L;

        producerId = -1;

        resetEvent();
    }

    private void resetEvent()
    {
        positionAsKey = false;
        key = -1L;

        metadataWriter = metadataWriterInstance;
        valueWriter = null;

        bufferWriterInstance.reset();
        metadataWriterInstance.reset();
    }

}
