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
package io.zeebe.logstreams.log;

import io.zeebe.logstreams.impl.CompleteEventsInBlockProcessor;
import io.zeebe.logstreams.impl.LogEntryDescriptor;
import io.zeebe.logstreams.impl.LoggedEventImpl;
import io.zeebe.logstreams.impl.log.index.LogBlockIndex;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.util.CloseableSilently;
import io.zeebe.util.allocation.AllocatedBuffer;
import io.zeebe.util.allocation.BufferAllocator;
import io.zeebe.util.allocation.DirectBufferAllocator;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static io.zeebe.logstreams.spi.LogStorage.OP_RESULT_INSUFFICIENT_BUFFER_CAPACITY;

public final class BufferedLogStreamReader implements LogStreamReader, CloseableSilently
{
    static final int DEFAULT_INITIAL_BUFFER_CAPACITY = 1024 * 32;

    private enum IteratorState
    {
        UNINITIALIZED,
        INITIALIZED,
        INITIALIZED_EMPTY_LOG,
        ACTIVE,
        NOT_COMMITTED;
    }

    private final LoggedEventImpl curr = new LoggedEventImpl();

    private final DirectBuffer buffer = new UnsafeBuffer(0, 0);

    private boolean readUncommittedEntries;

    private CompleteEventsInBlockProcessor completeEventsInBlockProcessor = new CompleteEventsInBlockProcessor();
    private LogStream logStream;
    private LogStorage logStorage;
    private LogBlockIndex blockIndex;

    private final BufferAllocator bufferAllocator = new DirectBufferAllocator();
    private AllocatedBuffer allocatedBuffer;
    private ByteBuffer ioBuffer;
    private long nextReadAddr;

    private final int initialBufferCapacity;

    private IteratorState iteratorState = IteratorState.UNINITIALIZED;

    public BufferedLogStreamReader()
    {
        this(DEFAULT_INITIAL_BUFFER_CAPACITY);
    }

    public BufferedLogStreamReader(boolean readUncommittedEntries)
    {
        this(DEFAULT_INITIAL_BUFFER_CAPACITY, readUncommittedEntries);
    }

    public BufferedLogStreamReader(int initialBufferCapacity)
    {
        this(initialBufferCapacity, false);
    }

    public BufferedLogStreamReader(int initialBufferCapacity, boolean readUncommittedEntries)
    {
        this.initialBufferCapacity = initialBufferCapacity;
        init();

        this.readUncommittedEntries = readUncommittedEntries;
    }

    private void init()
    {
        if (allocatedBuffer != null && !allocatedBuffer.isClosed())
        {
            allocatedBuffer.close();
        }
        this.allocatedBuffer = bufferAllocator.allocate(initialBufferCapacity);
        this.ioBuffer = allocatedBuffer.getRawBuffer();
        this.buffer.wrap(ioBuffer);
    }

    public BufferedLogStreamReader(int initialBufferCapacity, LogStream logStream)
    {
        this(initialBufferCapacity);
        wrap(logStream);
    }

    public BufferedLogStreamReader(int initialBufferCapacity, LogStream logStream, boolean readUncommittedEntries)
    {
        this(initialBufferCapacity, readUncommittedEntries);
        wrap(logStream);
    }

    public BufferedLogStreamReader(LogStream logStream)
    {
        this(DEFAULT_INITIAL_BUFFER_CAPACITY, logStream);
    }

    public BufferedLogStreamReader(LogStream logStream, boolean readUncommittedEntries)
    {
        this(DEFAULT_INITIAL_BUFFER_CAPACITY, logStream, readUncommittedEntries);
    }

    public BufferedLogStreamReader(LogStorage logStorage, LogBlockIndex blockIndex)
    {
        this(DEFAULT_INITIAL_BUFFER_CAPACITY);
        this.readUncommittedEntries = true;
        wrap(logStorage, blockIndex);
    }

    @Override
    public void wrap(LogStream logStream)
    {
        initReader(logStream);
        seekToFirstEvent();
    }

    @Override
    public void wrap(LogStream logStream, long position)
    {
        initReader(logStream);
        seek(position);
    }

    public void wrap(LogStorage logStorage, LogBlockIndex blockIndex)
    {
        initReader(logStorage, blockIndex);
        seekToFirstEvent();
    }

    private void initReader(LogStream logStream)
    {
        if (allocatedBuffer == null || isClosed())
        {
            init();
        }

        final LogStorage logStorage = logStream.getLogStorage();
        final LogBlockIndex blockIndex = logStream.getLogBlockIndex();

        this.logStorage = logStorage;
        this.blockIndex = blockIndex;
        this.logStream = logStream;
    }

    private void initReader(LogStorage logStorage, LogBlockIndex blockIndex)
    {
        this.logStorage = logStorage;
        this.blockIndex = blockIndex;
        this.logStream = null;
    }

    private void clear()
    {
        curr.wrap(buffer, -1);
        nextReadAddr = -1;
        iteratorState = IteratorState.UNINITIALIZED;
    }

    @Override
    public boolean seek(long seekPosition)
    {
        clear();

        final long commitPosition = getCommitPosition();
        if (commitPosition < 0)
        {
            // negative commit position -> nothing is committed
            iteratorState = IteratorState.INITIALIZED_EMPTY_LOG;
            return false;
        }

        nextReadAddr = blockIndex.lookupBlockAddress(seekPosition);
        if (nextReadAddr < 0)
        {
            // fallback: seek without index
            nextReadAddr = logStorage.getFirstBlockAddress();

            if (nextReadAddr == -1)
            {
                this.iteratorState = IteratorState.INITIALIZED_EMPTY_LOG;
                return false;
            }
        }

        // read
        final int readBytes = readBlockAt(nextReadAddr);

        if (readBytes == 0)
        {
            return false;
        }

        iteratorState = IteratorState.INITIALIZED;
        do
        {
            final LoggedEvent entry = next();
            final long entryPosition = entry.getPosition();

            if (entryPosition >= seekPosition)
            {
                iteratorState = IteratorState.INITIALIZED;
                return entryPosition == seekPosition;
            }

        }
        while (hasNext());

        iteratorState = IteratorState.ACTIVE;
        return false;
    }

    @Override
    public void seekToFirstEvent()
    {
        final int size = blockIndex.size();

        if (size > 0)
        {
            final long seekPosition = blockIndex.getLogPosition(0);
            seek(seekPosition);

            if (iteratorState == IteratorState.ACTIVE)
            {
                iteratorState = IteratorState.INITIALIZED;
            }

        }
        else
        {
            // fallback: seek without index
            seek(Long.MIN_VALUE);
        }
    }

    @Override
    public void seekToLastEvent()
    {
        final long commitPosition = getCommitPosition();

        seek(commitPosition);

        if (iteratorState == IteratorState.ACTIVE)
        {
            // will return last entry again
            this.iteratorState = IteratorState.INITIALIZED;
        }
    }

    private int readBlockAt(long readAddress)
    {
        prepareForNextRead();

        final int positionBeforeRead = ioBuffer.position();
        long opResult = 0;
        do
        {
            if (opResult == OP_RESULT_INSUFFICIENT_BUFFER_CAPACITY)
            {
                resizeBuffer(ioBuffer.capacity() * 2);
            }

            opResult = logStorage.read(ioBuffer, readAddress, completeEventsInBlockProcessor);

        } while (opResult == OP_RESULT_INSUFFICIENT_BUFFER_CAPACITY);

        final int readBytes = ioBuffer.position() - positionBeforeRead;
        if (opResult >= 0)
        {
            nextReadAddr = opResult;
        }
        else
        {
            iteratorState = IteratorState.ACTIVE;
            ioBuffer.limit(positionBeforeRead);
        }
        return readBytes;
    }

    private void prepareForNextRead()
    {
        final int initialPosition = curr.getFragmentOffset();

        if (initialPosition > 0)
        {
            ioBuffer.position(initialPosition);
            ioBuffer.compact();
        }
        else
        {
            iteratorState = IteratorState.INITIALIZED;
            ioBuffer.clear();
        }
        curr.wrap(buffer, 0);
    }

    private void resizeBuffer(int requiredCapacity)
    {
        final int pos = ioBuffer.position();
        final AllocatedBuffer newAllocatedBuffer = bufferAllocator.allocate(requiredCapacity);
        final ByteBuffer newBuffer = newAllocatedBuffer.getRawBuffer();
        if (pos > 0)
        {
            // copy remaining data
            ioBuffer.flip();
            newBuffer.put(ioBuffer);
        }

        newBuffer.limit(newBuffer.capacity());
        newBuffer.position(pos);

        allocatedBuffer.close();
        allocatedBuffer = newAllocatedBuffer;
        ioBuffer = newBuffer;
        buffer.wrap(ioBuffer);
    }

    @Override
    public boolean hasNext()
    {
        ensureInitialized();

        if (iteratorState == IteratorState.INITIALIZED)
        {
            return true;
        }

        if (iteratorState == IteratorState.INITIALIZED_EMPTY_LOG)
        {
            seekToFirstEvent();
            return iteratorState == IteratorState.INITIALIZED;
        }

        if (iteratorState == IteratorState.NOT_COMMITTED)
        {
            final long currentPosition = curr.getPosition();
            if (canReadPosition(currentPosition))
            {
                iteratorState = IteratorState.INITIALIZED;
                return true;
            }
            else
            {
                return false;
            }
        }

        final int fragmentLength = curr.getFragmentLength();
        int nextFragmentOffset = curr.getFragmentOffset() + fragmentLength;

        if (ioBuffer.limit() <= nextFragmentOffset)
        {
            final int readBytes = readBlockAt(nextReadAddr);
            if (readBytes == 0)
            {
                return false;
            }
            nextFragmentOffset = fragmentLength;
        }
//
//        final int nextHeaderEnd = nextFragmentOffset + headerLength;
//
//        if (available < nextHeaderEnd)
//        {
//            // Attempt to read at least next header
//            if (readMore(nextHeaderEnd - available))
//            {
//                // reading more data moved offset of next fragment to the left
//                nextFragmentOffset = fragmentLength;
//            }
//            else
//            {
//                return false;
//            }
//        }

//        final int nextFragmentLength = alignedLength(buffer.getInt(lengthOffset(nextFragmentOffset)));
//        final int nextFragmentEnd = nextFragmentOffset + nextFragmentLength;
//
//        if (available < nextFragmentEnd)
//        {
//            // Attempt to read remainder of fragment
//            if (!readMore(nextFragmentEnd - available))
//            {
//                return false;
//            }
//            nextFragmentOffset = fragmentLength;
//        }

        final long nextFragmentPosition = LogEntryDescriptor.getPosition(buffer, nextFragmentOffset);

        return canReadPosition(nextFragmentPosition);
    }

    private boolean canReadPosition(long position)
    {
        final long commitPosition = getCommitPosition();
        return commitPosition >= position;
    }

    private long getCommitPosition()
    {
        long commitPosition = Long.MAX_VALUE;

        if (!readUncommittedEntries)
        {
            commitPosition = logStream.getCommitPosition();
        }

        return commitPosition;
    }

    private void ensureInitialized()
    {
        if (iteratorState == IteratorState.UNINITIALIZED)
        {
            throw new IllegalStateException("Iterator not initialized");
        }
    }

    @Override
    public LoggedEvent next()
    {
        if (!hasNext())
        {
            throw new NoSuchElementException("Api protocol violation: No next log entry available; You need to probe with hasNext() first.");
        }

        if (iteratorState == IteratorState.INITIALIZED)
        {
            iteratorState = IteratorState.ACTIVE;
            return curr;
        }
        else
        {
            final int offset = curr.getFragmentOffset();
            final int fragmentLength = curr.getFragmentLength();

            final int nextFragmentOffset = offset + fragmentLength;
//            if (nextFragmentOffset >= ioBuffer.limit())
//            {
//                final int readBytes = readBlockAt(nextReadAddr);
//
//                if (readBytes == 0)
//                {
//                    throw new NoSuchElementException("Api protocol violation: No next log entry available; You need to probe with hasNext() first.");
//                }
//
//                if (readBytes < ioBuffer.limit())
//                {
//                    final int newOffset = ioBuffer.limit() - readBytes;
//                    curr.wrap(buffer, newOffset);
//                }
//            }
//            else
//            {
            curr.wrap(buffer, nextFragmentOffset);
//            }

            return curr;
        }
    }

    @Override
    public long getPosition()
    {
        long position = -1L;

        if (iteratorState == IteratorState.INITIALIZED || iteratorState == IteratorState.ACTIVE)
        {
            position = curr.getPosition();
        }

        return position;
    }

    public boolean isClosed()
    {
        return allocatedBuffer.isClosed();
    }

    @Override
    public void close()
    {
        allocatedBuffer.close();
    }
}
