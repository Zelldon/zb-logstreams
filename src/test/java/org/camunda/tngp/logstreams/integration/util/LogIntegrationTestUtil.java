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
package org.camunda.tngp.logstreams.integration.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.camunda.tngp.logstreams.log.BufferedLogStreamReader;
import org.camunda.tngp.logstreams.log.LogStream;
import org.camunda.tngp.logstreams.log.LogStreamReader;
import org.camunda.tngp.logstreams.log.LogStreamWriter;
import org.camunda.tngp.logstreams.log.LoggedEvent;

public class LogIntegrationTestUtil
{

    public static void writeLogEvents(final LogStream log, final int workCount, int messageSize, final int offset)
    {
        final LogStreamWriter writer = new LogStreamWriter(log);

        final UnsafeBuffer msg = new UnsafeBuffer(ByteBuffer.allocate(messageSize));

        for (int i = 0; i < workCount; i++)
        {
            msg.putInt(0, offset + i);

            writer
                .key(offset + i)
                .value(msg);

            while (writer.tryWrite() < 0)
            {
                // spin
            }
        }
    }

    public static void waitUntilWrittenKey(final LogStream log, final int key)
    {
        final BufferedLogStreamReader logReader = new BufferedLogStreamReader(log);

        logReader.seekToLastEvent();

        long entryKey = 0;
        while (entryKey < key - 1)
        {
            if (logReader.hasNext())
            {
                final LoggedEvent nextEntry = logReader.next();
                entryKey = nextEntry.getLongKey();
            }
        }
    }

    public static void waitUntilWrittenEvents(final LogStream log, final int eventCount)
    {
        final BufferedLogStreamReader logReader = new BufferedLogStreamReader(log);

        long count = 0;
        while (count < eventCount)
        {
            if (logReader.hasNext())
            {
                logReader.next();

                count += 1;
            }
        }
    }

    public static void readLogAndAssertEvents(final LogStreamReader logReader, final int workCount, int messageSize)
    {
        int count = 0;
        long lastPosition = -1L;
        long lastKey = -1L;

        while (count < workCount)
        {
            if (logReader.hasNext())
            {
                final LoggedEvent entry = logReader.next();
                final long currentPosition = entry.getPosition();
                final long currentKey = entry.getLongKey();

                assertThat(currentPosition > lastPosition);
                assertThat(currentKey).isGreaterThan(lastKey);

                final DirectBuffer valueBuffer = entry.getValueBuffer();
                final long value = valueBuffer.getInt(entry.getValueOffset());
                assertThat(value).isEqualTo(entry.getLongKey());
                assertThat(entry.getValueLength()).isEqualTo(messageSize);

                lastPosition = currentPosition;
                lastKey = currentKey;

                count++;
            }
        }

        assertThat(count).isEqualTo(workCount);
        assertThat(lastKey).isEqualTo(workCount - 1);

        assertThat(logReader.hasNext()).isFalse();
    }

}