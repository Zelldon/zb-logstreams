package org.camunda.tngp.logstreams.log;

import static org.agrona.BitUtil.*;
import static org.camunda.tngp.dispatcher.impl.log.LogBufferAppender.*;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.*;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.camunda.tngp.dispatcher.ClaimedFragment;
import org.camunda.tngp.dispatcher.Dispatcher;
import org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor;
import org.camunda.tngp.util.EnsureUtil;
import org.camunda.tngp.util.buffer.BufferWriter;
import org.camunda.tngp.util.buffer.DirectBufferWriter;

public class LogStreamWriter
{
    protected final DirectBufferWriter metadataWriterInstance = new DirectBufferWriter();
    protected final DirectBufferWriter bufferWriterInstance = new DirectBufferWriter();
    protected final ClaimedFragment claimedFragment = new ClaimedFragment();
    protected Dispatcher logWriteBuffer;
    protected int logId;

    protected boolean positionAsKey;
    protected long key;

    protected long sourceEventPosition = -1L;
    protected int sourceEventLogStreamId = -1;

    protected int producerId = -1;

    protected final short keyLength = SIZE_OF_LONG;

    protected BufferWriter metadataWriter;

    protected BufferWriter valueWriter;

    public LogStreamWriter()
    {
    }

    public LogStreamWriter(LogStream log)
    {
        wrap(log);
    }

    public void wrap(LogStream log)
    {
        final StreamContext logContext = log.getContext();

        this.logWriteBuffer = logContext.getWriteBuffer();
        this.logId = logContext.getLogId();

        reset();
    }

    public LogStreamWriter positionAsKey()
    {
        positionAsKey = true;
        return this;
    }

    public LogStreamWriter key(long key)
    {
        this.key = key;
        return this;
    }

    public LogStreamWriter sourceEvent(int logStreamId, long position)
    {
        this.sourceEventLogStreamId = logStreamId;
        this.sourceEventPosition = position;
        return this;
    }

    public LogStreamWriter producerId(int producerId)
    {
        this.producerId = producerId;
        return this;
    }

    public LogStreamWriter metadata(DirectBuffer buffer, int offset, int length)
    {
        metadataWriterInstance.wrap(buffer, offset, length);
        return this;
    }

    public LogStreamWriter metadata(DirectBuffer buffer)
    {
        return metadata(buffer, 0, buffer.capacity());
    }

    public LogStreamWriter metadataWriter(BufferWriter writer)
    {
        this.metadataWriter = writer;
        return this;
    }

    public LogStreamWriter value(DirectBuffer value, int valueOffset, int valueLength)
    {
        return valueWriter(bufferWriterInstance.wrap(value, valueOffset, valueLength));
    }

    public LogStreamWriter value(DirectBuffer value)
    {
        return value(value, 0, value.capacity());
    }

    public LogStreamWriter valueWriter(BufferWriter writer)
    {
        this.valueWriter = writer;
        return this;
    }

    public void reset()
    {
        positionAsKey = false;
        key = -1L;
        metadataWriter = metadataWriterInstance;
        valueWriter = null;
        sourceEventLogStreamId = -1;
        sourceEventPosition = -1L;
        producerId = -1;

        bufferWriterInstance.reset();
        metadataWriterInstance.reset();
    }

    /**
     * Attempts to write the event to the underlying stream.
     *
     * @return the event position or a negative value if fails to write the
     *         event
     */
    public long tryWrite()
    {
        EnsureUtil.ensureNotNull("value", valueWriter);
        if (!positionAsKey)
        {
            EnsureUtil.ensureGreaterThanOrEqual("key", key, 0);
        }

        long result = -1;

        final int valueLength = valueWriter.getLength();
        final int metadataLength = metadataWriter.getLength();

        // claim fragment in log write buffer
        final long claimedPosition = claimLogEntry(valueLength, keyLength, metadataLength);

        if (claimedPosition >= 0)
        {
            try
            {
                final MutableDirectBuffer writeBuffer = claimedFragment.getBuffer();
                final int bufferOffset = claimedFragment.getOffset();
                final int keyOffset = keyOffset(bufferOffset);
                final int metadataLengthOffset = keyOffset + keyLength;
                final int metadataOffset = metadataLengthOffset + METADATA_HEADER_LENGTH;
                final int valueWriteOffset = metadataOffset + metadataLength;
                final long keyToWrite = positionAsKey ? claimedPosition : key;

                // write log entry header
                writeBuffer.putLong(positionOffset(bufferOffset), claimedPosition);

                writeBuffer.putInt(producerIdOffset(bufferOffset), producerId);

                writeBuffer.putInt(sourceEventLogStreamIdOffset(bufferOffset), sourceEventLogStreamId);
                writeBuffer.putLong(sourceEventPositionOffset(bufferOffset), sourceEventPosition);

                writeBuffer.putShort(keyTypeOffset(bufferOffset), KEY_TYPE_UINT64);
                writeBuffer.putShort(keyLengthOffset(bufferOffset), keyLength);
                writeBuffer.putLong(keyOffset, keyToWrite);

                writeBuffer.putShort(metadataLengthOffset, (short) metadataLength);
                if (metadataLength > 0)
                {
                    metadataWriter.write(writeBuffer, metadataOffset);
                }

                // write log entry
                valueWriter.write(writeBuffer, valueWriteOffset);

                result = claimedPosition;
                claimedFragment.commit();
            }
            catch (Exception e)
            {
                claimedFragment.abort();
                LangUtil.rethrowUnchecked(e);
            }
            finally
            {
                reset();
            }
        }

        return result;
    }

    private long claimLogEntry(final int valueLength, final short keyLength, final int metadataLength)
    {
        final int framedLength = valueLength + headerLength(keyLength, metadataLength);

        long claimedPosition = -1;

        do
        {
            claimedPosition = logWriteBuffer.claim(claimedFragment, framedLength, logId);
        }
        while (claimedPosition == RESULT_PADDING_AT_END_OF_PARTITION);

        return claimedPosition - DataFrameDescriptor.alignedLength(framedLength);
    }

}