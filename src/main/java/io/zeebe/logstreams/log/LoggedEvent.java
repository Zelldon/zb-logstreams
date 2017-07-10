/**
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
package io.zeebe.logstreams.log;

import org.agrona.DirectBuffer;
import io.zeebe.util.buffer.BufferReader;

/**
 * Represents an event on the log stream.
 */
public interface LoggedEvent
{
    /**
     * @return the event's position in the log.
     */
    long getPosition();

    /**
     * @return the offset of the log stream topic name of the event which caused this event
     */
    int getSourceEventLogStreamTopicNameOffset();

    /**
     * @return the length the log stream topic name of the event which caused this event
     */
    short getSourceEventLogStreamTopicNameLength();

    /**
     * @return a buffer containing the log stream topic name of the event which caused this event
     *          at offset {@link #getSourceEventLogStreamTopicNameOffset()}
     *          and with length {@link #getSourceEventLogStreamTopicNameLength()}.
     */
    DirectBuffer getSourceEventLogStreamTopicName();

    /**
     * Wraps the given buffer to read the log stream topic name of the event which caused this event
     *
     * @param reader
     *          the reader to read into
     */
    void readSourceEventLogStreamTopicName(BufferReader reader);

    /**
     * @return the log stream topic partition id of the event which causes this event. Returns a
     *         negative value if no such an event exists.
     */
    int getSourceEventLogStreamPartitionId();

    /**
     * @return the position of the event which causes this event. Returns a
     *         negative value if no such an event exists.
     */
    long getSourceEventPosition();

    /**
     * @return the id of the producer which produced this event
     */
    int getProducerId();

    /**
     * @return the key of the event
     */
    long getKey();

    /**
     * @return a buffer containing the event's metadata at offset
     *         {@link #getMetadataOffset()} and with length {@link #getMetadataLength()}.
     */
    DirectBuffer getMetadata();

    /**
     * @return the offset of the event's metadata
     */
    int getMetadataOffset();

    /**
     * @return the length of the event's metadata
     */
    short getMetadataLength();

    /**
     * Wraps the given buffer to read the event's metadata
     *
     * @param reader
     *          the reader to read into
     */
    void readMetadata(BufferReader reader);

    /**
     * @return a buffer containing the value of the event at offset
     *         {@link #getValueOffset()} ()} and with length {@link #getValueLength()} ()}.
     */
    DirectBuffer getValueBuffer();

    /**
     * @return the buffer offset where the event's value can read from
     */
    int getValueOffset();

    /**
     * @return the length of the event's value
     */
    int getValueLength();

    /**
     * Wraps the given buffer to read the event's value.
     *
     * @param reader
     *            the buffer to read from
     */
    void readValue(BufferReader reader);

}
