package org.camunda.tngp.logstreams.log;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.camunda.tngp.logstreams.impl.CompleteAndCommittedEventsInBlockProcessor;
import org.camunda.tngp.logstreams.impl.log.fs.FsLogStorage;
import org.camunda.tngp.logstreams.impl.log.fs.FsLogStorageConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor.*;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.*;
import static org.camunda.tngp.logstreams.spi.LogStorage.OP_RESULT_INSUFFICIENT_BUFFER_CAPACITY;

/**
 * @author Christopher Zell <christopher.zell@camunda.com>
 */
public class CompleteAndCommittedEventsInBlockProcessorTest
{

    private static final int SEGMENT_SIZE = 1024 * 16;

    protected static final int LENGTH = headerLength(0, 0);
    protected static final int ALIGNED_LEN = alignedLength(LENGTH);

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private CompleteAndCommittedEventsInBlockProcessor processor;
    private String logPath;
    private FsLogStorageConfiguration fsStorageConfig;
    private FsLogStorage fsLogStorage;
    private long appendedAddress;

    @Before
    public void init()
    {
        processor = new CompleteAndCommittedEventsInBlockProcessor();
        logPath = tempFolder.getRoot().getAbsolutePath();
        fsStorageConfig = new FsLogStorageConfiguration(SEGMENT_SIZE, logPath, 0, false);
        fsLogStorage = new FsLogStorage(fsStorageConfig);

        final ByteBuffer writeBuffer = ByteBuffer.allocate(128);
        final MutableDirectBuffer directBuffer = new UnsafeBuffer(0, 0);
        directBuffer.wrap(writeBuffer);

        /*
         Buffer: [4test4asdf30012345678901234567890123456789]
         */
        //small events
        int idx = 0;
        directBuffer.putInt(lengthOffset(idx), LENGTH);
        directBuffer.putLong(positionOffset(messageOffset(idx)), 1);

        idx = ALIGNED_LEN;
        directBuffer.putInt(idx, LENGTH);
        directBuffer.putLong(positionOffset(messageOffset(idx)), 2);

        // a large event
        idx = 2 * ALIGNED_LEN;
        directBuffer.putInt(idx, headerLength(96, 96)); // aligned size: 48
        directBuffer.putLong(positionOffset(messageOffset(idx)), 3);

        fsLogStorage.open();
        appendedAddress = fsLogStorage.append(writeBuffer);
    }

    @Test
    public void shouldReadAndProcessFirstEvent()
    {
        // given buffer, which could contain first event
        processor.setCommitPosition(1);
        final ByteBuffer readBuffer = ByteBuffer.allocate(ALIGNED_LEN);

        // when read into buffer and buffer was processed
        final long result = fsLogStorage.read(readBuffer, appendedAddress, processor);

        // then
        // result is equal to start address plus event size
        assertThat(result).isEqualTo(appendedAddress + ALIGNED_LEN);
        final DirectBuffer buffer = new UnsafeBuffer(0, 0);
        buffer.wrap(readBuffer);

        // first event was read
        assertThat(buffer.getInt(0)).isEqualTo(LENGTH);
        assertThat(getPosition(buffer, 0)).isEqualTo(1);
    }

    @Test
    public void shouldNotReadFirstEventIfNoCommitPositionIsSet()
    {
        // given buffer, which could contain first event
        final ByteBuffer readBuffer = ByteBuffer.allocate(ALIGNED_LEN);

        // when read into buffer and buffer was processed
        final long result = fsLogStorage.read(readBuffer, appendedAddress, processor);

        // then
        // result is equal to start address no event was read
        assertThat(result).isEqualTo(appendedAddress);
        final DirectBuffer buffer = new UnsafeBuffer(0, 0);
        buffer.wrap(readBuffer);

        // first event was read
        assertThat(readBuffer.position()).isEqualTo(0);
        assertThat(readBuffer.limit()).isEqualTo(readBuffer.capacity());
    }

    @Test
    public void shouldNotReadSecondEventIfCommitPositionIsSetToFirst()
    {
        // given buffer, which could contain both event's
        processor.setCommitPosition(1);
        final ByteBuffer readBuffer = ByteBuffer.allocate(2 * ALIGNED_LEN);

        // when read into buffer and buffer was processed
        final long result = fsLogStorage.read(readBuffer, appendedAddress, processor);

        // then
        // result is equal to start address plus first event length
        assertThat(result).isEqualTo(appendedAddress + ALIGNED_LEN);
        final DirectBuffer buffer = new UnsafeBuffer(0, 0);
        buffer.wrap(readBuffer);

        // first event was read
        assertThat(readBuffer.position()).isEqualTo(ALIGNED_LEN);
        assertThat(readBuffer.limit()).isEqualTo(ALIGNED_LEN);
        assertThat(buffer.getInt(0)).isEqualTo(LENGTH);
        assertThat(getPosition(buffer, 0)).isEqualTo(1);
    }

    @Test
    public void shouldReadAndProcessTwoEvents()
    {
        // given buffer, which could contain 2 events
        processor.setCommitPosition(2);
        final ByteBuffer readBuffer = ByteBuffer.allocate(2 * ALIGNED_LEN);

        // when read into buffer and buffer was processed
        final long result = fsLogStorage.read(readBuffer, appendedAddress, processor);

        // then
        // returned address is equal to start address plus two event sizes
        assertThat(result).isEqualTo(appendedAddress + ALIGNED_LEN * 2);
        final DirectBuffer buffer = new UnsafeBuffer(0, 0);
        buffer.wrap(readBuffer);

        // first event was read
        assertThat(buffer.getInt(0)).isEqualTo(LENGTH);
        assertThat(getPosition(buffer, 0)).isEqualTo(1);

        // second event was read as well
        assertThat(buffer.getInt(ALIGNED_LEN)).isEqualTo(LENGTH);
        assertThat(getPosition(buffer, ALIGNED_LEN)).isEqualTo(2);
    }

    @Test
    public void shouldTruncateHalfEvent()
    {
        // given buffer, which could contain 1.5 events
        processor.setCommitPosition(2);
        final ByteBuffer readBuffer = ByteBuffer.allocate((int) (ALIGNED_LEN * 1.5));

        // when read into buffer and buffer was processed
        final long result = fsLogStorage.read(readBuffer, appendedAddress, processor);

        // then
        // result is equal to start address plus one event size
        assertThat(result).isEqualTo(appendedAddress + ALIGNED_LEN);
        final DirectBuffer buffer = new UnsafeBuffer(0, 0);
        buffer.wrap(readBuffer);

        // and only first event is read
        assertThat(buffer.getInt(0)).isEqualTo(LENGTH);
        assertThat(getPosition(buffer, 0)).isEqualTo(1);

        // position and limit is reset
        assertThat(readBuffer.position()).isEqualTo(ALIGNED_LEN);
        assertThat(readBuffer.limit()).isEqualTo(ALIGNED_LEN);
    }

    @Test
    public void shouldTruncateEventWithMissingLen()
    {
        // given buffer, which could contain one event and only 3 next bits
        // so not the complete next message len
        processor.setCommitPosition(2);
        final ByteBuffer readBuffer = ByteBuffer.allocate((ALIGNED_LEN + 3));

        // when read into buffer and buffer was processed
        final long result = fsLogStorage.read(readBuffer, appendedAddress, processor);

        // then
        // result is equal to start address plus one event size
        assertThat(result).isEqualTo(appendedAddress + ALIGNED_LEN);
        final DirectBuffer buffer = new UnsafeBuffer(0, 0);
        buffer.wrap(readBuffer);

        // and only first event is read
        assertThat(buffer.getInt(0)).isEqualTo(LENGTH);
        assertThat(getPosition(buffer, 0)).isEqualTo(1);

        // position and limit is reset
        assertThat(readBuffer.position()).isEqualTo(ALIGNED_LEN);
        assertThat(readBuffer.limit()).isEqualTo(ALIGNED_LEN);
    }

    @Test
    public void shouldInsufficientBufferCapacity()
    {
        // given buffer, which could not contain an event
        final ByteBuffer readBuffer = ByteBuffer.allocate((ALIGNED_LEN - 1));

        // when read into buffer and buffer was processed
        final long result = fsLogStorage.read(readBuffer, appendedAddress, processor);

        // then result is OP_RESULT_INSUFFICIENT_BUFFER_CAPACITY
        assertThat(result).isEqualTo(OP_RESULT_INSUFFICIENT_BUFFER_CAPACITY);
    }

    @Test
    public void shouldInsufficientBufferCapacityForLessThenHalfFullBuffer()
    {
        // given buffer
        processor.setCommitPosition(3);
        final ByteBuffer readBuffer = ByteBuffer.allocate(4 * ALIGNED_LEN + 1);

        // when read into buffer and buffer was processed
        final long result = fsLogStorage.read(readBuffer, appendedAddress, processor);

        // then only first 2 small events can be read
        // third event was to large, since position is less then remaining bytes,
        // which means buffer is less then half full, OP_RESULT_INSUFFICIENT_BUFFER_CAPACITY will be returned
        assertThat(result).isEqualTo(OP_RESULT_INSUFFICIENT_BUFFER_CAPACITY);
    }

    @Test
    public void shouldTruncateBufferOnHalfBufferWasRead()
    {
        // given buffer
        processor.setCommitPosition(3);
        final ByteBuffer readBuffer = ByteBuffer.allocate(alignedLength(headerLength(96, 96)));

        // when read into buffer and buffer was processed
        final long result = fsLogStorage.read(readBuffer, appendedAddress, processor);

        // then only first 2 small events can be read
        // third event was to large, since position is EQUAL to remaining bytes,
        // which means buffer is half full, the corresponding next address will be returned
        // and block idx can for example be created
        assertThat(result).isEqualTo(appendedAddress + ALIGNED_LEN * 2);
        final DirectBuffer buffer = new UnsafeBuffer(0, 0);
        buffer.wrap(readBuffer);

        // first event was read
        assertThat(buffer.getInt(0)).isEqualTo(LENGTH);
        assertThat(getPosition(buffer, 0)).isEqualTo(1);

        // second event was read as well
        assertThat(buffer.getInt(ALIGNED_LEN)).isEqualTo(LENGTH);
        assertThat(getPosition(buffer, ALIGNED_LEN)).isEqualTo(2);

        // position and limit is reset
        assertThat(readBuffer.position()).isEqualTo(2 * ALIGNED_LEN);
        assertThat(readBuffer.limit()).isEqualTo(2 * ALIGNED_LEN);
    }
}