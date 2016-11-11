package org.camunda.tngp.logstreams.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor.HEADER_LENGTH;
import static org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor.alignedLength;
import static org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor.lengthOffset;
import static org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor.messageOffset;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.positionOffset;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.agrona.concurrent.UnsafeBuffer;
import org.camunda.tngp.logstreams.impl.LogBlockIndex;
import org.camunda.tngp.logstreams.spi.LogStorage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

public class LogBlockIndexTest
{
    private static final int CAPACITY = 111;
    private static final int INDEX_BLOCK_SIZE = 1024;

    private LogBlockIndex blockIndex;

    @Mock
    private LogStorage mockLogStorage;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup()
    {
        MockitoAnnotations.initMocks(this);

        blockIndex = createNewBlockIndex(CAPACITY);
    }

    protected LogBlockIndex createNewBlockIndex(int capacity)
    {
        return new LogBlockIndex(capacity, c ->
        {
            return new UnsafeBuffer(ByteBuffer.allocate(c));
        });
    }

    @Test
    public void shouldAddBlock()
    {
        final int capacity = blockIndex.capacity();

        // when

        for (int i = 0; i < capacity; i++)
        {
            final int pos = i + 1;
            final int addr = pos * 10;
            final int expectedSize = i + 1;

            assertThat(blockIndex.addBlock(pos, addr)).isEqualTo(expectedSize);
            assertThat(blockIndex.size()).isEqualTo(expectedSize);
        }

        // then

        for (int i = 0; i < capacity; i++)
        {
            final int virtPos = i + 1;
            final int physPos = virtPos * 10;

            assertThat(blockIndex.getLogPosition(i)).isEqualTo(virtPos);
            assertThat(blockIndex.getAddress(i)).isEqualTo(physPos);
        }

    }

    @Test
    public void shouldNotAddBlockIfCapacityReached()
    {
        // given
        final int capacity = blockIndex.capacity();

        while (capacity > blockIndex.size())
        {
            blockIndex.addBlock(blockIndex.size(), 0);
        }

        // then
        exception.expect(RuntimeException.class);
        exception.expectMessage(String.format("LogBlockIndex capacity of %d entries reached. Cannot add new block.", capacity));

        // when
        blockIndex.addBlock(blockIndex.size(), 0);
    }

    @Test
    public void shouldNotAddBlockWithEqualPos()
    {
        // given
        blockIndex.addBlock(10, 0);

        // then
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Illegal value for position");

        // when
        blockIndex.addBlock(10, 0);
    }

    @Test
    public void shouldNotAddBlockWithSmallerPos()
    {
        // given
        blockIndex.addBlock(10, 0);

        // then
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Illegal value for position");

        // when
        blockIndex.addBlock(9, 0);
    }

    @Test
    public void shouldReturnMinusOneForEmptyBlockIndex()
    {
        assertThat(blockIndex.lookupBlockAddress(-1)).isEqualTo(-1);
        assertThat(blockIndex.lookupBlockAddress(1)).isEqualTo(-1);
    }

    @Test
    public void shouldLookupBlocks()
    {
        final int capacity = blockIndex.capacity();

        // given

        for (int i = 0; i < capacity; i++)
        {
            final int pos = (i + 1) * 10;
            final int addr = (i + 1) * 100;

            blockIndex.addBlock(pos, addr);
        }

        // then

        for (int i = 0; i < capacity; i++)
        {
            final int expectedAddr = (i + 1) * 100;

            for (int j = 0; j < 10; j++)
            {
                final int pos = ((i + 1) * 10) + j;

                assertThat(blockIndex.lookupBlockAddress(pos)).isEqualTo(expectedAddr);
            }
        }
    }

    @Test
    public void shouldRecoverIndexFromSnapshot() throws Exception
    {
        final int capacity = blockIndex.capacity();

        for (int i = 0; i < capacity; i++)
        {
            final int pos = i + 1;
            final int addr = pos * 10;

            blockIndex.addBlock(pos, addr);
        }

        // when
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        blockIndex.writeSnapshot(outputStream);

        final LogBlockIndex newBlockIndex = createNewBlockIndex(CAPACITY);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        newBlockIndex.recoverFromSnapshot(inputStream);

        // then
        assertThat(newBlockIndex.size()).isEqualTo(blockIndex.size());

        for (int i = 0; i < capacity; i++)
        {
            final int virtPos = i + 1;
            final int physPos = virtPos * 10;

            assertThat(newBlockIndex.getLogPosition(i)).isEqualTo(virtPos);
            assertThat(newBlockIndex.getAddress(i)).isEqualTo(physPos);
        }
    }

    @Test
    public void shouldRecoverIndexFromLogStorage()
    {
        when(mockLogStorage.getFirstBlockAddress()).thenReturn(1L);

        // only create index if index block size is reached
        when(mockLogStorage.read(any(ByteBuffer.class), eq(1L))).thenAnswer(readLogEntry(11, 2, INDEX_BLOCK_SIZE / 4));
        when(mockLogStorage.read(any(ByteBuffer.class), eq(2L))).thenAnswer(readLogEntry(12, 3, INDEX_BLOCK_SIZE / 4));
        when(mockLogStorage.read(any(ByteBuffer.class), eq(3L))).thenAnswer(readLogEntry(13, 4, INDEX_BLOCK_SIZE - HEADER_LENGTH));
        when(mockLogStorage.read(any(ByteBuffer.class), eq(4L))).thenAnswer(readLogEntry(14, 5, INDEX_BLOCK_SIZE / 2));
        when(mockLogStorage.read(any(ByteBuffer.class), eq(5L))).thenAnswer(readLogEntry(15, 6, INDEX_BLOCK_SIZE - HEADER_LENGTH));

        blockIndex.recover(mockLogStorage, INDEX_BLOCK_SIZE);

        assertThat(blockIndex.size()).isEqualTo(2);

        assertThat(blockIndex.getAddress(0)).isEqualTo(3);
        assertThat(blockIndex.getLogPosition(0)).isEqualTo(13);

        assertThat(blockIndex.getAddress(1)).isEqualTo(5);
        assertThat(blockIndex.getLogPosition(1)).isEqualTo(15);
    }

    @Test
    public void shouldRecoverIndexFromLogStorageWithMultipleLogEntriesPerBlock()
    {
        when(mockLogStorage.getFirstBlockAddress()).thenReturn(1L);

        // create index for first log entry in block if index block size is reached
        when(mockLogStorage.read(any(ByteBuffer.class), eq(1L))).thenAnswer(readLogEntry(11, 2, (INDEX_BLOCK_SIZE - HEADER_LENGTH) / 2));
        when(mockLogStorage.read(any(ByteBuffer.class), eq(2L))).thenAnswer(readLogEntries(12, 4, (INDEX_BLOCK_SIZE - HEADER_LENGTH) / 3, 2));
        when(mockLogStorage.read(any(ByteBuffer.class), eq(4L))).thenAnswer(readLogEntry(14, 5, (INDEX_BLOCK_SIZE - HEADER_LENGTH) / 2));
        when(mockLogStorage.read(any(ByteBuffer.class), eq(5L))).thenAnswer(readLogEntries(15, 8, (INDEX_BLOCK_SIZE - HEADER_LENGTH) / 4, 3));

        blockIndex.recover(mockLogStorage, INDEX_BLOCK_SIZE);

        assertThat(blockIndex.size()).isEqualTo(2);

        assertThat(blockIndex.getAddress(0)).isEqualTo(2);
        assertThat(blockIndex.getLogPosition(0)).isEqualTo(12);

        assertThat(blockIndex.getAddress(1)).isEqualTo(5);
        assertThat(blockIndex.getLogPosition(1)).isEqualTo(15);
    }

    @Test
    public void shouldRecoverIndexFromLogStorageWithOverlappingLogEntryMessage()
    {
        when(mockLogStorage.getFirstBlockAddress()).thenReturn(1L);

        // second log entry message overlaps the block
        when(mockLogStorage.read(any(ByteBuffer.class), eq(1L))).thenAnswer(readLogEntries(11, 3, (int) (0.8 * INDEX_BLOCK_SIZE), 2));
        // the remaining part of the log entry
        when(mockLogStorage.read(any(ByteBuffer.class), eq(3L))).thenAnswer(readLogEntry(12, 4, (int) (INDEX_BLOCK_SIZE * 0.6)));
        // next log entry
        when(mockLogStorage.read(any(ByteBuffer.class), eq(4L))).thenAnswer(readLogEntry(13, 5, INDEX_BLOCK_SIZE - HEADER_LENGTH));

        blockIndex.recover(mockLogStorage, INDEX_BLOCK_SIZE);

        assertThat(blockIndex.size()).isEqualTo(2);

        assertThat(blockIndex.getAddress(0)).isEqualTo(1);
        assertThat(blockIndex.getLogPosition(0)).isEqualTo(11);

        assertThat(blockIndex.getAddress(1)).isEqualTo(4);
        assertThat(blockIndex.getLogPosition(1)).isEqualTo(13);
    }

    @Test
    public void shouldRecoverIndexFromLogStorageWithOverlappingLogEntryHeader()
    {
        when(mockLogStorage.getFirstBlockAddress()).thenReturn(1L);

        // second log entry header overlaps the block
        when(mockLogStorage.read(any(ByteBuffer.class), eq(1L))).thenAnswer(readLogEntries(11, 3, INDEX_BLOCK_SIZE - 2 * HEADER_LENGTH, 2));
        // the remaining part of the second log entry header
        when(mockLogStorage.read(any(ByteBuffer.class), eq(3L))).thenAnswer(readLogEntry(12, 4, HEADER_LENGTH));
        // the second log entry message
        when(mockLogStorage.read(any(ByteBuffer.class), eq(4L))).thenAnswer(readLogEntry(12, 5, INDEX_BLOCK_SIZE - HEADER_LENGTH));
        // next log entry
        when(mockLogStorage.read(any(ByteBuffer.class), eq(5L))).thenAnswer(readLogEntry(13, 6, INDEX_BLOCK_SIZE - HEADER_LENGTH));

        blockIndex.recover(mockLogStorage, INDEX_BLOCK_SIZE);

        assertThat(blockIndex.size()).isEqualTo(2);

        assertThat(blockIndex.getAddress(0)).isEqualTo(1);
        assertThat(blockIndex.getLogPosition(0)).isEqualTo(11);

        assertThat(blockIndex.getAddress(1)).isEqualTo(5);
        assertThat(blockIndex.getLogPosition(1)).isEqualTo(13);
    }

    @Test
    public void shouldRecoverIndexFromSnapshotAndLogStorage() throws Exception
    {
        // add one block and create snapshot
        blockIndex.addBlock(11, 1L);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        blockIndex.writeSnapshot(outputStream);

        when(mockLogStorage.read(any(ByteBuffer.class), eq(1L))).thenAnswer(readLogEntry(11, 2, INDEX_BLOCK_SIZE - HEADER_LENGTH));
        // add one more block after snapshot
        when(mockLogStorage.read(any(ByteBuffer.class), eq(2L))).thenAnswer(readLogEntry(12, 3, INDEX_BLOCK_SIZE - HEADER_LENGTH));

        final LogBlockIndex newBlockIndex = createNewBlockIndex(CAPACITY);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        newBlockIndex.recoverFromSnapshot(inputStream);

        newBlockIndex.recover(mockLogStorage, 11, INDEX_BLOCK_SIZE);

        assertThat(newBlockIndex.size()).isEqualTo(2);

        assertThat(newBlockIndex.getAddress(0)).isEqualTo(1);
        assertThat(newBlockIndex.getLogPosition(0)).isEqualTo(11);

        assertThat(newBlockIndex.getAddress(1)).isEqualTo(2);
        assertThat(newBlockIndex.getLogPosition(1)).isEqualTo(12);
    }

    protected Answer<Integer> readLogEntry(long logPosition, int nextAddress, int messageLength)
    {
        return readLogEntries(logPosition, nextAddress, messageLength, 1);
    }

    protected Answer<Integer> readLogEntries(long logPosition, int nextAddress, int messageLength, int entryCount)
    {
        return invocation ->
        {
            final ByteBuffer byteBuffer = (ByteBuffer) invocation.getArguments()[0];
            final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);

            int offset = 0;

            for (int i = 0; i < entryCount; i++)
            {
                buffer.putInt(lengthOffset(offset), messageLength);

                if (messageOffset(offset) <= byteBuffer.limit())
                {
                    buffer.putLong(positionOffset(messageOffset(offset)), logPosition + i);
                }

                offset += alignedLength(messageLength);
            }

            byteBuffer.position(Math.min(offset, byteBuffer.limit()));

            return nextAddress;
        };
    }

}
