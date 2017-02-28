package org.camunda.tngp.logstreams.log;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.camunda.tngp.logstreams.impl.CompleteEventsInBlockProcessor;
import org.camunda.tngp.logstreams.impl.log.fs.FsLogStorage;
import org.camunda.tngp.logstreams.impl.log.fs.FsLogStorageConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor.alignedLength;
import static org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor.messageOffset;
import static org.camunda.tngp.logstreams.spi.LogStorage.OP_RESULT_INSUFFICIENT_BUFFER_CAPACITY;

/**
 * @author Christopher Zell <christopher.zell@camunda.com>
 */
public class CompleteEventsInBlockProcessorTest
{

    private static final int SEGMENT_SIZE = 1024 * 16;

    private static final String MSG = "test";
    private static final byte[] MSG_BYTES = MSG.getBytes();
    protected static final int ALIGNED_LEN = alignedLength(MSG_BYTES.length);

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    final CompleteEventsInBlockProcessor processor = new CompleteEventsInBlockProcessor();

    private String logPath;
    private File logDirectory;
    private FsLogStorageConfiguration fsStorageConfig;
    private FsLogStorage fsLogStorage;
    private long appendedAddress;

    @Before
    public void init()
    {
        logPath = tempFolder.getRoot().getAbsolutePath();
        logDirectory = new File(logPath);

        fsStorageConfig = new FsLogStorageConfiguration(SEGMENT_SIZE, logPath, 0, false);
        fsLogStorage = new FsLogStorage(fsStorageConfig);

        final ByteBuffer writeBuffer = ByteBuffer.allocate(128);
        final MutableDirectBuffer directBuffer = new UnsafeBuffer(0, 0);
        directBuffer.wrap(writeBuffer);

        /*
         Buffer: [8test8test2401234567890123456789]
         */
        //small events
        for (int i = 0; i < 2; i++)
        {
            final int idx = i * ALIGNED_LEN;
            directBuffer.putInt(idx, MSG_BYTES.length);
            directBuffer.putBytes(messageOffset(idx), MSG_BYTES);
        }
        // a large event
        final int idx = 2 * ALIGNED_LEN;
        final String msg = "012345678901234567890123456789"; // 30
        directBuffer.putInt(idx, msg.length()); // aligned size: 48
        directBuffer.putBytes(messageOffset(idx), msg.getBytes());
        fsLogStorage.open();
        appendedAddress = fsLogStorage.append(writeBuffer);
    }

    @Test
    public void shouldReadAndProcessFirstEvent()
    {
        // given buffer, which could contain first event
        final ByteBuffer readBuffer = ByteBuffer.allocate(ALIGNED_LEN);

        // when read into buffer and buffer was processed
        final long result = fsLogStorage.read(readBuffer, appendedAddress, processor);

        // then
        // result is equal to start address plus event size
        assertThat(result).isEqualTo(appendedAddress + ALIGNED_LEN);
        final DirectBuffer buffer = new UnsafeBuffer(0, 0);
        buffer.wrap(readBuffer);

        // first event was read
        assertThat(buffer.getInt(0)).isEqualTo(MSG_BYTES.length);
        final byte[] bytes = getBytes(readBuffer, messageOffset(0));
        assertThat(bytes).isEqualTo(MSG_BYTES);
    }

    @Test
    public void shouldReadAndProcessTwoEvents()
    {
        // given buffer, which could contain 2 events
        final ByteBuffer readBuffer = ByteBuffer.allocate(2 * ALIGNED_LEN);

        // when read into buffer and buffer was processed
        final long result = fsLogStorage.read(readBuffer, appendedAddress, processor);

        // then
        // returned address is equal to start address plus two event sizes
        assertThat(result).isEqualTo(appendedAddress + ALIGNED_LEN * 2);
        final DirectBuffer buffer = new UnsafeBuffer(0, 0);
        buffer.wrap(readBuffer);

        // first event was read
        assertThat(buffer.getInt(0)).isEqualTo(MSG_BYTES.length);
        byte[] bytes = getBytes(readBuffer, messageOffset(0));
        assertThat(bytes).isEqualTo(MSG_BYTES);

        // second event was read as well
        assertThat(buffer.getInt(ALIGNED_LEN)).isEqualTo(MSG_BYTES.length);
        bytes = getBytes(readBuffer, messageOffset(ALIGNED_LEN));
        assertThat(bytes).isEqualTo(MSG_BYTES);
    }

    @Test
    public void shouldTruncateHalfEvent()
    {
        // given buffer, which could contain 1.5 events
        final ByteBuffer readBuffer = ByteBuffer.allocate((int) (ALIGNED_LEN * 1.5));

        // when read into buffer and buffer was processed
        final long result = fsLogStorage.read(readBuffer, appendedAddress, processor);

        // then
        // result is equal to start address plus one event size
        assertThat(result).isEqualTo(appendedAddress + ALIGNED_LEN);
        final DirectBuffer buffer = new UnsafeBuffer(0, 0);
        buffer.wrap(readBuffer);

        // and only first event is read
        assertThat(buffer.getInt(0)).isEqualTo(MSG_BYTES.length);
        final byte[] bytes = getBytes(readBuffer, messageOffset(0));
        assertThat(bytes).isEqualTo(MSG_BYTES);

        // rest is empty / clear
        assertClearBuffer(readBuffer, ALIGNED_LEN);
    }

    @Test
    public void shouldTruncateEventWithMissingLen()
    {
        // given buffer, which could contain one event and only 3 next bits
        // so not the complete next message len
        final ByteBuffer readBuffer = ByteBuffer.allocate((ALIGNED_LEN + 3));

        // when read into buffer and buffer was processed
        final long result = fsLogStorage.read(readBuffer, appendedAddress, processor);

        // then
        // result is equal to start address plus one event size
        assertThat(result).isEqualTo(appendedAddress + ALIGNED_LEN);
        final DirectBuffer buffer = new UnsafeBuffer(0, 0);
        buffer.wrap(readBuffer);

        // and only first event is read
        assertThat(buffer.getInt(0)).isEqualTo(MSG_BYTES.length);
        final byte[] bytes = getBytes(readBuffer, messageOffset(0));
        assertThat(bytes).isEqualTo(MSG_BYTES);

        // rest is empty / clear
        assertClearBuffer(readBuffer, ALIGNED_LEN);
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
        final ByteBuffer readBuffer = ByteBuffer.allocate(4 * ALIGNED_LEN);

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
        assertThat(buffer.getInt(0)).isEqualTo(MSG_BYTES.length);
        byte[] bytes = getBytes(readBuffer, messageOffset(0));
        assertThat(bytes).isEqualTo(MSG_BYTES);

        // second event was read as well
        assertThat(buffer.getInt(ALIGNED_LEN)).isEqualTo(MSG_BYTES.length);
        bytes = getBytes(readBuffer, messageOffset(ALIGNED_LEN));
        assertThat(bytes).isEqualTo(MSG_BYTES);

        // rest is empty / clear
        assertClearBuffer(readBuffer, 2 * ALIGNED_LEN);
    }

    private void assertClearBuffer(ByteBuffer readBuffer, int offSet)
    {
        final byte b = 0;
        for (int i = 0; i < readBuffer.capacity() - offSet; i++)
        {
            assertThat(readBuffer.get(i + offSet)).isEqualTo(b);
        }
    }

    private byte[] getBytes(ByteBuffer readBuffer, int startIdx)
    {
        final byte[] bytes = new byte[MSG_BYTES.length];
        for (int i = 0; i < MSG_BYTES.length; i++)
        {
            bytes[i] = readBuffer.get(startIdx + i);
        }
        return bytes;
    }

}