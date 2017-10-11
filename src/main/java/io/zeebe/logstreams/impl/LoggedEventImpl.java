/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.logstreams.impl;

import org.agrona.DirectBuffer;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.util.buffer.BufferReader;

import static io.zeebe.dispatcher.impl.log.DataFrameDescriptor.*;
import static io.zeebe.logstreams.impl.LogEntryDescriptor.headerLength;

/**
 * Represents the implementation of the logged event.
 */
public class LoggedEventImpl implements ReadableFragment, LoggedEvent
{
    protected int fragmentOffset = -1;
    protected int messageOffset = -1;
    protected DirectBuffer buffer;

    public void wrap(final DirectBuffer buffer, final int offset)
    {
        this.fragmentOffset = offset;
        this.messageOffset = messageOffset(fragmentOffset);
        this.buffer = buffer;
    }

    @Override
    public int getType()
    {
        return buffer.getShort(typeOffset(fragmentOffset));
    }

    @Override
    public int getVersion()
    {
        return buffer.getShort(versionOffset(fragmentOffset));
    }

    @Override
    public int getMessageLength()
    {
        return messageLength(buffer.getInt(lengthOffset(fragmentOffset)));
    }

    @Override
    public int getMessageOffset()
    {
        return messageOffset;
    }

    @Override
    public int getStreamId()
    {
        return buffer.getInt(streamIdOffset(fragmentOffset));
    }

    @Override
    public DirectBuffer getBuffer()
    {
        return buffer;
    }

    public int getFragmentLength()
    {
        return LogEntryDescriptor.getFragmentLength(buffer, fragmentOffset);
    }

    public int getFragmentOffset()
    {
        return fragmentOffset;
    }

    @Override
    public long getPosition()
    {
        return LogEntryDescriptor.getPosition(buffer, fragmentOffset);
    }

    @Override
    public int getRaftTerm()
    {
        return LogEntryDescriptor.getRaftTerm(buffer, messageOffset);
    }

    @Override
    public long getKey()
    {
        return LogEntryDescriptor.getKey(buffer, messageOffset);
    }

    @Override
    public DirectBuffer getMetadata()
    {
        return buffer;
    }

    @Override
    public short getMetadataLength()
    {
        return LogEntryDescriptor.getMetadataLength(buffer, messageOffset);
    }

    @Override
    public int getMetadataOffset()
    {
        return LogEntryDescriptor.metadataOffset(messageOffset);
    }

    @Override
    public void readMetadata(final BufferReader reader)
    {
        reader.wrap(buffer, getMetadataOffset(), getMetadataLength());
    }

    @Override
    public int getValueOffset()
    {
        final short metadataLength = getMetadataLength();
        return LogEntryDescriptor.valueOffset(messageOffset, metadataLength);
    }

    @Override
    public int getValueLength()
    {
        final short metadataLength = getMetadataLength();

        return getMessageLength() - headerLength(metadataLength);
    }

    @Override
    public DirectBuffer getValueBuffer()
    {
        return buffer;
    }

    @Override
    public void readValue(final BufferReader reader)
    {
        reader.wrap(buffer, getValueOffset(), getValueLength());
    }

    @Override
    public int getSourceEventLogStreamPartitionId()
    {
        return LogEntryDescriptor.getSourceEventLogStreamPartitionId(buffer, messageOffset);
    }

    @Override
    public long getSourceEventPosition()
    {
        return LogEntryDescriptor.getSourceEventPosition(buffer, messageOffset);
    }

    @Override
    public int getProducerId()
    {
        return LogEntryDescriptor.getProducerId(buffer, messageOffset);
    }

    @Override
    public String toString()
    {
        return "LoggedEvent [type=" +
            getType() +
            ", version=" +
            getVersion() +
            ", streamId=" +
            getStreamId() +
            ", position=" +
            getPosition() +
            ", key=" +
            getKey() +
            ", sourceEventLogStreamPartitionId=" +
            getSourceEventLogStreamPartitionId() +
            ", sourceEventPosition=" +
            getSourceEventPosition() +
            ", producerId=" +
            getProducerId() +
            "]";
    }
}
