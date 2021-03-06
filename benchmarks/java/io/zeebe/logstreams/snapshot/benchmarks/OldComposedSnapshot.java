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
package io.zeebe.logstreams.snapshot.benchmarks;

import io.zeebe.logstreams.spi.SnapshotSupport;
import io.zeebe.util.StreamUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import static org.agrona.BitUtil.SIZE_OF_INT;

/**
 * A composition of one or more snapshots which are combined to a single snapshot.
 */
public class OldComposedSnapshot implements SnapshotSupport
{
    protected final SnapshotSupport[] parts;
    protected final int count;
    protected long processedBytes;

    protected final UnsafeBuffer headerBuffer;
    protected final UnsafeBuffer readBuffer = new UnsafeBuffer(0, 0);

    public OldComposedSnapshot(SnapshotSupport... parts)
    {
        this.parts = parts;
        this.count = parts.length;

        if (count < 1)
        {
            throw new IllegalArgumentException("must contains at least one part");
        }

        final int headerLength = (1 + count) * SIZE_OF_INT;
        headerBuffer = new UnsafeBuffer(new byte[headerLength]);
    }

    public long getProcessedBytes()
    {
        return processedBytes;
    }

    @Override
    public void writeSnapshot(OutputStream outputStream) throws Exception
    {
        long writtenBytes = 0;
        try (ByteArrayOutputStream snapshotDataBuffer = new ByteArrayOutputStream())
        {
            int offset = 0;

            headerBuffer.putInt(offset, count);
            offset += SIZE_OF_INT;
            writtenBytes = SIZE_OF_INT;

            for (int i = 0; i < count; i++)
            {
                final int dataOffset = snapshotDataBuffer.size();

                parts[i].writeSnapshot(snapshotDataBuffer);

                final int dataLength = snapshotDataBuffer.size() - dataOffset;
                headerBuffer.putInt(offset, dataLength);
                offset += SIZE_OF_INT;
                writtenBytes += SIZE_OF_INT;
                writtenBytes += dataLength;
            }

            outputStream.write(headerBuffer.byteArray());
            snapshotDataBuffer.writeTo(outputStream);
        }
        processedBytes = writtenBytes;
    }

    @Override
    public void recoverFromSnapshot(InputStream inputStream) throws Exception
    {
        final byte[] buffer = StreamUtil.read(inputStream);
        readBuffer.wrap(buffer);

        int offset = 0;
        long readBytes;

        final int dataCount = readBuffer.getInt(offset);
        offset += SIZE_OF_INT;
        readBytes = SIZE_OF_INT;

        if (dataCount != count)
        {
            throw new IllegalStateException("illegal data of composed snapshot, expected " + count + " parts but found " + dataCount);
        }

        final int[] dataLength = new int[count];
        for (int i = 0; i < count; i++)
        {
            dataLength[i] =  readBuffer.getInt(offset);
            offset += SIZE_OF_INT;
            readBytes += SIZE_OF_INT;
        }

        for (int i = 0; i < count; i++)
        {
            final int length = dataLength[i];
            readBytes += length;

            try (InputStream dataInputStream = new ByteArrayInputStream(buffer, offset, length))
            {
                parts[i].recoverFromSnapshot(dataInputStream);
            }

            offset += length;
        }
        processedBytes = readBytes;
    }

    @Override
    public void reset()
    {
        for (int i = 0; i < count; i++)
        {
            parts[i].reset();
        }
    }

}
