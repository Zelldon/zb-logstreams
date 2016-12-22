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

import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.tngp.logstreams.log.MockLogStorage.newLogEntries;
import static org.camunda.tngp.logstreams.log.MockLogStorage.newLogEntry;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.NoSuchElementException;

import org.agrona.DirectBuffer;
import org.camunda.tngp.logstreams.impl.log.index.LogBlockIndex;
import org.camunda.tngp.util.buffer.DirectBufferReader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class BufferedLogStreamReaderTest
{
    @Mock
    private LogBlockIndex mockBlockIndex;

    @Mock
    private LogStream mockLogStream;

    private MockLogStorage mockLogStorage;

    private BufferedLogStreamReader reader;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void init()
    {
        MockitoAnnotations.initMocks(this);

        mockLogStorage = new MockLogStorage();

        final StreamContext streamContext = new StreamContext();
        streamContext.setLogStorage(mockLogStorage.getMock());
        streamContext.setBlockIndex(mockBlockIndex);

        when(mockLogStream.getContext()).thenReturn(streamContext);

        reader = new BufferedLogStreamReader();
    }

    @Test
    public void shouldReadEvent()
    {
        when(mockBlockIndex.size()).thenReturn(1);
        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.lookupBlockAddress(1L)).thenReturn(10L);

        mockLogStorage.add(newLogEntry()
                .address(10)
                .position(1)
                .key(2)
                .sourceEventLogStreamId(3)
                .sourceEventPosition(4L)
                .producerId(5)
                .value("event".getBytes()));

        reader.wrap(mockLogStream);

        final boolean hasNext = reader.hasNext();
        assertThat(hasNext).isTrue();

        final LoggedEvent event = reader.next();

        assertThat(event).isNotNull();
        assertThat(event.getPosition()).isEqualTo(1L);
        assertThat(event.getLongKey()).isEqualTo(2L);

        assertThat(event.getSourceEventLogStreamId()).isEqualTo(3);
        assertThat(event.getSourceEventPosition()).isEqualTo(4L);

        assertThat(event.getProducerId()).isEqualTo(5);

        final DirectBufferReader readBuffer = new DirectBufferReader();
        event.readValue(readBuffer);

        assertThat(readBuffer.byteArray()).isEqualTo("event".getBytes());
    }

    @Test
    public void shouldReadEventValueBuffer()
    {
        when(mockBlockIndex.size()).thenReturn(1);
        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.lookupBlockAddress(1L)).thenReturn(10L);

        mockLogStorage.add(newLogEntry().address(10).value("event".getBytes()));

        reader.wrap(mockLogStream);

        final boolean hasNext = reader.hasNext();
        assertThat(hasNext).isTrue();

        final LoggedEvent event = reader.next();

        final DirectBuffer valueBuffer = event.getValueBuffer();
        final byte[] readValueBuffer = new byte[event.getValueLength()];
        valueBuffer.getBytes(event.getValueOffset(), readValueBuffer);

        assertThat(readValueBuffer).isEqualTo("event".getBytes());
    }

    @Test
    public void shouldReadEventMetadataBuffer()
    {
        when(mockBlockIndex.size()).thenReturn(1);
        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.lookupBlockAddress(1L)).thenReturn(10L);

        mockLogStorage.add(newLogEntry().address(10).metadata("metadata".getBytes()).value("event".getBytes()));

        reader.wrap(mockLogStream);

        final boolean hasNext = reader.hasNext();
        assertThat(hasNext).isTrue();

        final LoggedEvent event = reader.next();

        final DirectBuffer metadataBuffer = event.getMetadata();
        final byte[] readMetadataBuffer = new byte[event.getMetadataLength()];
        metadataBuffer.getBytes(event.getMetadataOffset(), readMetadataBuffer);

        assertThat(readMetadataBuffer).isEqualTo("metadata".getBytes());

        // AND: value can also be read

        final DirectBuffer valueBuffer = event.getValueBuffer();
        final byte[] readValueBuffer = new byte[event.getValueLength()];
        valueBuffer.getBytes(event.getValueOffset(), readValueBuffer);

        assertThat(readValueBuffer).isEqualTo("event".getBytes());
    }

    @Test
    public void shouldReadFirstEvent()
    {
        when(mockBlockIndex.size()).thenReturn(2);

        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.getLogPosition(1)).thenReturn(5L);

        when(mockBlockIndex.lookupBlockAddress(1L)).thenReturn(10L);
        when(mockBlockIndex.lookupBlockAddress(5L)).thenReturn(15L);

        mockLogStorage
            .add(newLogEntries(4).address(10).position(1).nextAddress(15))
            .add(newLogEntries(3).address(15).position(5));

        reader.wrap(mockLogStream);

        reader.hasNext();
        final LoggedEvent event = reader.next();

        assertThat(event.getPosition()).isEqualTo(1L);
    }

    @Test
    public void shouldReadEventAtPosition()
    {
        when(mockBlockIndex.size()).thenReturn(2);

        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.getLogPosition(1)).thenReturn(5L);

        when(mockBlockIndex.lookupBlockAddress(1L)).thenReturn(10L);
        when(mockBlockIndex.lookupBlockAddress(3L)).thenReturn(10L);
        when(mockBlockIndex.lookupBlockAddress(5L)).thenReturn(15L);

        mockLogStorage
            .add(newLogEntries(4).address(10).position(1).nextAddress(15))
            .add(newLogEntries(3).address(15).position(5));

        reader.wrap(mockLogStream, 3);

        reader.hasNext();
        final LoggedEvent event = reader.next();

        assertThat(event.getPosition()).isEqualTo(3L);
    }

    @Test
    public void shouldIterateOverTheLog()
    {
        when(mockBlockIndex.size()).thenReturn(2);

        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.getLogPosition(1)).thenReturn(5L);

        when(mockBlockIndex.lookupBlockAddress(1L)).thenReturn(10L);
        when(mockBlockIndex.lookupBlockAddress(5L)).thenReturn(15L);

        mockLogStorage
            .add(newLogEntries(4).address(10).position(1).nextAddress(15))
            .add(newLogEntries(3).address(15).position(5));

        reader.wrap(mockLogStream);

        long position = 1;
        while (reader.hasNext())
        {
            final LoggedEvent event = reader.next();
            assertThat(event.getPosition()).isEqualTo(position);
            position += 1;
        }
        assertThat(position).isEqualTo(8);
    }

    @Test
    public void shouldSeekToFirstEvent()
    {
        when(mockBlockIndex.size()).thenReturn(2);

        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.getLogPosition(1)).thenReturn(5L);

        when(mockBlockIndex.lookupBlockAddress(1L)).thenReturn(10L);
        when(mockBlockIndex.lookupBlockAddress(5L)).thenReturn(15L);

        mockLogStorage
            .add(newLogEntries(4).address(10).position(1).nextAddress(15))
            .add(newLogEntries(3).address(15).position(5));

        reader.wrap(mockLogStream, 5);

        reader.seekToFirstEvent();

        reader.hasNext();
        final LoggedEvent event = reader.next();

        assertThat(event.getPosition()).isEqualTo(1L);
        assertThat(reader.getPosition()).isEqualTo(1L);
    }

    @Test
    public void shouldSeekToLastEvent()
    {
        when(mockBlockIndex.size()).thenReturn(2);

        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.getLogPosition(1)).thenReturn(5L);

        when(mockBlockIndex.lookupBlockAddress(1L)).thenReturn(10L);
        when(mockBlockIndex.lookupBlockAddress(5L)).thenReturn(15L);

        mockLogStorage
            .add(newLogEntries(4).address(10).position(1).nextAddress(15))
            .add(newLogEntries(3).address(15).position(5));

        reader.wrap(mockLogStream);

        reader.seekToLastEvent();

        reader.hasNext();
        final LoggedEvent event = reader.next();

        assertThat(event.getPosition()).isEqualTo(7L);
        assertThat(reader.getPosition()).isEqualTo(7L);
    }

    @Test
    public void shouldSeekToPosition()
    {
        when(mockBlockIndex.size()).thenReturn(2);

        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.getLogPosition(1)).thenReturn(5L);

        when(mockBlockIndex.lookupBlockAddress(1L)).thenReturn(10L);
        when(mockBlockIndex.lookupBlockAddress(3L)).thenReturn(10L);
        when(mockBlockIndex.lookupBlockAddress(5L)).thenReturn(15L);

        mockLogStorage
            .add(newLogEntries(4).address(10).position(1).nextAddress(15))
            .add(newLogEntries(3).address(15).position(5));

        reader.wrap(mockLogStream);

        final boolean found = reader.seek(3);

        reader.hasNext();
        final LoggedEvent event = reader.next();

        assertThat(found).isTrue();
        assertThat(event.getPosition()).isEqualTo(3L);
        assertThat(reader.getPosition()).isEqualTo(3L);
    }

    @Test
    public void shouldSeekToNextPositionIfNotExist()
    {
        when(mockBlockIndex.size()).thenReturn(2);

        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.getLogPosition(1)).thenReturn(5L);

        when(mockBlockIndex.lookupBlockAddress(1L)).thenReturn(10L);
        when(mockBlockIndex.lookupBlockAddress(3L)).thenReturn(10L);
        when(mockBlockIndex.lookupBlockAddress(5L)).thenReturn(15L);

        mockLogStorage
            .add(newLogEntries(2).address(10).position(1).nextAddress(15))
            .add(newLogEntries(3).address(15).position(5));

        reader.wrap(mockLogStream);

        final boolean found = reader.seek(3);

        reader.hasNext();
        final LoggedEvent event = reader.next();

        assertThat(found).isFalse();
        assertThat(event.getPosition()).isEqualTo(5L);
        assertThat(reader.getPosition()).isEqualTo(5L);
    }

    @Test
    public void shouldSeekToFirstEventIfIndexNotExist()
    {
        when(mockBlockIndex.size()).thenReturn(0);

        mockLogStorage
            .firstBlockAddress(10)
            .add(newLogEntries(4).address(10).position(1).nextAddress(15))
            .add(newLogEntries(3).address(15).position(5));

        reader.wrap(mockLogStream);

        reader.seekToFirstEvent();

        final boolean hasNext = reader.hasNext();
        assertThat(hasNext).isTrue();

        final LoggedEvent event = reader.next();

        assertThat(event.getPosition()).isEqualTo(1L);
        assertThat(reader.getPosition()).isEqualTo(1L);
    }

    @Test
    public void shouldSeekToLastEventIfIndexNotExist()
    {
        when(mockBlockIndex.size()).thenReturn(0);

        mockLogStorage
            .firstBlockAddress(10)
            .add(newLogEntries(4).address(10).position(1).nextAddress(15))
            .add(newLogEntries(3).address(15).position(5));

        reader.wrap(mockLogStream);

        reader.seekToLastEvent();

        final boolean hasNext = reader.hasNext();
        assertThat(hasNext).isTrue();

        final LoggedEvent event = reader.next();

        assertThat(event.getPosition()).isEqualTo(7L);
        assertThat(reader.getPosition()).isEqualTo(7L);
    }

    @Test
    public void shouldSeekToPositionIfIndexNotExist()
    {
        when(mockBlockIndex.size()).thenReturn(0);

        mockLogStorage
            .firstBlockAddress(10)
            .add(newLogEntries(4).address(10).position(1).nextAddress(15))
            .add(newLogEntries(3).address(15).position(5));

        reader.wrap(mockLogStream);

        reader.seek(3);

        final boolean hasNext = reader.hasNext();
        assertThat(hasNext).isTrue();

        final LoggedEvent event = reader.next();

        assertThat(event.getPosition()).isEqualTo(3L);
        assertThat(reader.getPosition()).isEqualTo(3L);
    }

    @Test
    public void shouldNotHasEventIfPositionNotExist()
    {
        when(mockBlockIndex.size()).thenReturn(1);

        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);

        when(mockBlockIndex.lookupBlockAddress(1L)).thenReturn(10L);
        when(mockBlockIndex.lookupBlockAddress(3L)).thenReturn(10L);

        mockLogStorage.add(newLogEntries(2).address(10).position(1));

        reader.wrap(mockLogStream);

        final boolean found = reader.seek(3);

        final boolean hasNext = reader.hasNext();

        assertThat(found).isFalse();
        assertThat(hasNext).isFalse();
    }

    @Test
    public void shouldBufferEvents()
    {
        when(mockBlockIndex.size()).thenReturn(1);
        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.lookupBlockAddress(1L)).thenReturn(10L);

        mockLogStorage.add(newLogEntries(4).address(10).position(1));

        reader.wrap(mockLogStream);

        // iterate over the log
        while (reader.hasNext())
        {
            reader.next();
        }

        // assert that the reader copies larger blocks from the log into the buffer to batch I/O
        verify(mockLogStorage.getMock(), times(1)).read(any(), eq(10L));
    }

    @Test
    public void shouldNotHasEventIfNotInitialized()
    {
        thrown.expect(IllegalStateException.class);

        reader.hasNext();
    }

    @Test
    public void shouldNotHasEventIfLogIsEmpty()
    {
        mockLogStorage.firstBlockAddress(-1);

        reader.wrap(mockLogStream);

        final boolean hasNext = reader.hasNext();

        assertThat(hasNext).isFalse();
    }

    @Test
    public void shouldNotReadEventIfLogIsEmpty()
    {
        mockLogStorage.firstBlockAddress(-1);

        reader.wrap(mockLogStream);

        reader.hasNext();

        thrown.expect(NoSuchElementException.class);

        reader.next();
    }

    @Test
    public void shouldNotGetPositionIfNotInitialized()
    {
        thrown.expect(IllegalStateException.class);

        reader.getPosition();
    }

    @Test
    public void shouldNotGetPositionIfLogIsEmpty()
    {
        mockLogStorage.firstBlockAddress(-1);

        reader.wrap(mockLogStream);

        thrown.expect(NoSuchElementException.class);

        reader.getPosition();
    }

}