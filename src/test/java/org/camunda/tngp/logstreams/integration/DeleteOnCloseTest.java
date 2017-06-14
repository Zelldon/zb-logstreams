package org.camunda.tngp.logstreams.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.tngp.util.buffer.BufferUtil.wrapString;

import java.io.File;
import java.util.concurrent.ExecutionException;

import org.agrona.DirectBuffer;
import org.camunda.tngp.logstreams.LogStreams;
import org.camunda.tngp.logstreams.log.LogStream;
import org.camunda.tngp.util.newagent.TaskScheduler;
import org.junit.*;
import org.junit.rules.TemporaryFolder;


public class DeleteOnCloseTest
{
    private static final DirectBuffer TOPIC_NAME = wrapString("test-topic");

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private TaskScheduler taskScheduler;

    @Before
    public void setup()
    {
        taskScheduler = TaskScheduler.createSingleThreadedScheduler();
    }

    @After
    public void destroy() throws Exception
    {
        taskScheduler.close();
    }

    @Test
    public void shouldNotDeleteOnCloseByDefault() throws InterruptedException, ExecutionException
    {
        final File logFolder = tempFolder.getRoot();

        final LogStream log = LogStreams.createFsLogStream(TOPIC_NAME, 0)
            .logRootPath(logFolder.getAbsolutePath())
            .taskScheduler(taskScheduler)
            .build();

        log.open();

        // if
        log.close();

        // then
        assertThat(logFolder.listFiles().length).isGreaterThan(0);
    }

    @Test
    @Ignore
    public void shouldDeleteOnCloseIfSet() throws InterruptedException, ExecutionException
    {
        final File logFolder = tempFolder.getRoot();

        final LogStream log = LogStreams.createFsLogStream(TOPIC_NAME, 0)
            .logRootPath(logFolder.getAbsolutePath())
            .deleteOnClose(true)
            .taskScheduler(taskScheduler)
            .build();

        log.open();

        // if
        log.close();

        // then
        assertThat(logFolder.listFiles().length).isEqualTo(0);
    }
}
