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
package io.zeebe.logstreams.integration;

import static io.zeebe.logstreams.integration.util.LogIntegrationTestUtil.*;
import static io.zeebe.util.buffer.BufferUtil.wrapString;

import java.nio.ByteBuffer;

import io.zeebe.logstreams.LogStreams;
import io.zeebe.logstreams.log.*;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class LogIntegrationTest
{
    private static final DirectBuffer TOPIC_NAME = wrapString("test-topic");
    private static final int MSG_SIZE = 911;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public ActorSchedulerRule actorScheduler = new ActorSchedulerRule();

    private LogStream logStream;

    @Before
    public void setup()
    {
        final String logPath = tempFolder.getRoot().getAbsolutePath();

        logStream = LogStreams.createFsLogStream(TOPIC_NAME, 0)
                .logRootPath(logPath)
                .deleteOnClose(true)
                .logSegmentSize(1024 * 1024 * 16)
                .actorScheduler(actorScheduler.get())
                .build();

        logStream.open();
        logStream.openLogStreamController().join();
    }

    @After
    public void destroy() throws Exception
    {
        logStream.close();
    }

    @Test
    public void shouldWriteEvents()
    {
        final int workPerIteration = 100_000;

        writeLogEvents(logStream, workPerIteration, MSG_SIZE, 0);

        final LogStreamReader logReader = new BufferedLogStreamReader(logStream, true);
        readLogAndAssertEvents(logReader, workPerIteration, MSG_SIZE);
        logReader.close();
    }

    @Test
    public void shouldWriteEventsAsBatch()
    {
        final UnsafeBuffer msg = new UnsafeBuffer(ByteBuffer.allocate(MSG_SIZE));

        final int batchSize = 1_000;
        final int eventSizePerBatch = 100;

        final LogStreamBatchWriter logStreamWriter = new LogStreamBatchWriterImpl(logStream);

        for (int i = 0; i < batchSize; i++)
        {
            for (int j = 0; j < eventSizePerBatch; j++)
            {
                final int key = i * eventSizePerBatch + j;

                msg.putInt(0, key);

                logStreamWriter.event()
                    .key(key)
                    .value(msg)
                    .done();
            }

            while (logStreamWriter.tryWrite() < 0)
            {
                // spin
            }
        }

        final LogStreamReader logStreamReader = new BufferedLogStreamReader(logStream, true);
        readLogAndAssertEvents(logStreamReader, batchSize * eventSizePerBatch, MSG_SIZE);
        logStreamReader.close();
    }

    @Test
    public void shouldWriteEventBatchesWithDifferentLengths()
    {
        final UnsafeBuffer msg = new UnsafeBuffer(ByteBuffer.allocate(MSG_SIZE));

        final int batchSize = 1_000;
        final int eventSizePerBatch = 100;

        final LogStreamBatchWriter logStreamWriter = new LogStreamBatchWriterImpl(logStream);

        int eventCount = 0;
        for (int i = 0; i < batchSize; i++)
        {
            for (int j = 0; j < 1 + (i % eventSizePerBatch); j++)
            {
                final int key = i * eventSizePerBatch + j;

                msg.putInt(0, key);

                logStreamWriter.event()
                    .key(key)
                    .value(msg, 0, MSG_SIZE - (j % 8))
                    .done();

                eventCount += 1;
            }

            while (logStreamWriter.tryWrite() < 0)
            {
                // spin
            }
        }

        waitUntilWrittenEvents(logStream, eventCount);
    }

}
