package org.camunda.tngp.logstreams.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.tngp.logstreams.integration.util.LogIntegrationTestUtil.waitUntilWrittenKey;
import static org.camunda.tngp.logstreams.integration.util.LogIntegrationTestUtil.writeLogEvents;
import static org.camunda.tngp.util.buffer.BufferUtil.wrapString;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.agrona.DirectBuffer;
import org.camunda.tngp.logstreams.LogStreams;
import org.camunda.tngp.logstreams.fs.FsLogStreamBuilder;
import org.camunda.tngp.logstreams.integration.util.LogIntegrationTestUtil;
import org.camunda.tngp.logstreams.log.BufferedLogStreamReader;
import org.camunda.tngp.logstreams.log.LogStream;
import org.camunda.tngp.logstreams.log.LogStreamReader;
import org.camunda.tngp.util.newagent.TaskScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LogRecoveryTest
{
    // ~ overall message size 46 MB
    private static final int MSG_SIZE = 911;
    private static final int WORK_COUNT = 50_000;

    private static final int LOG_SEGMENT_SIZE = 1024 * 1024 * 8;
    private static final int INDEX_BLOCK_SIZE = 1024 * 1024 * 2;

    private static final int WORK_COUNT_PER_BLOCK_IDX = (INDEX_BLOCK_SIZE / MSG_SIZE);

    private static final DirectBuffer TOPIC_NAME = wrapString("foo");
    private static final int PARTITION_ID = 0;

    @Rule
    public TemporaryFolder temFolder = new TemporaryFolder();

    private String logPath;

    private TaskScheduler taskScheduler;

    private FsLogStreamBuilder logStreamBuilder;

    @Before
    public void setup()
    {
        logPath = temFolder.getRoot().getAbsolutePath();

        taskScheduler = TaskScheduler.createSingleThreadedScheduler();

        logStreamBuilder = getLogStreamBuilder();
    }

    private FsLogStreamBuilder getLogStreamBuilder()
    {
        return LogStreams.createFsLogStream(TOPIC_NAME, PARTITION_ID)
            .logRootPath(logPath)
            .deleteOnClose(false)
            .logSegmentSize(LOG_SEGMENT_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE)
            .readBlockSize(INDEX_BLOCK_SIZE)
            .snapshotPolicy(pos -> false)
            .taskScheduler(taskScheduler);
    }

    @After
    public void destroy() throws Exception
    {
        taskScheduler.close();
    }

    @Test
    public void shouldRecoverIndexWithLogBlockIndexController() throws InterruptedException, ExecutionException
    {
        // given open log stream and written events
        final LogStream logStream = logStreamBuilder.build();
        logStream.setCommitPosition(Long.MAX_VALUE);
        logStream.open();
        writeLogEvents(logStream, WORK_COUNT, MSG_SIZE, 0);
        waitUntilWrittenKey(logStream, WORK_COUNT);

        // when log stream is closed
        logStream.close();

        // block index was created
        final int indexSize = logStream.getLogBlockIndex().size();
        final int calculatesIndexSize = calculateIndexSize(WORK_COUNT);
        assertThat(indexSize).isGreaterThan(calculatesIndexSize);

        // and can be recovered with new log stream
        readLogAndAssertEvents(WORK_COUNT, calculatesIndexSize);
    }

    @Test
    public void shouldNotRecoverIndexIfCommitPositionIsNotSet() throws InterruptedException, ExecutionException
    {
        // given block index for written events
        final LogStream logStream = logStreamBuilder.build();
        logStream.setCommitPosition(Long.MAX_VALUE);
        logStream.open();
        writeLogEvents(logStream, WORK_COUNT, MSG_SIZE, 0);
        waitUntilWrittenKey(logStream, WORK_COUNT);
        logStream.close();
        final int indexSize = logStream.getLogBlockIndex().size();
        final int calculatesIndexSize = calculateIndexSize(WORK_COUNT);
        assertThat(indexSize).isGreaterThan(calculatesIndexSize);

        // when new log stream is opened, without a commit position
        final LogStream newLog = LogStreams.createFsLogStream(TOPIC_NAME, PARTITION_ID)
            .logRootPath(logPath)
            .deleteOnClose(true)
            .logSegmentSize(LOG_SEGMENT_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE)
            .readBlockSize(INDEX_BLOCK_SIZE)
            .taskScheduler(taskScheduler)
            .logStreamControllerDisabled(true)
            .build();
        newLog.open();

        // then block index can't be recovered
        final LogStreamReader logReader = new BufferedLogStreamReader(newLog, true);
        LogIntegrationTestUtil.readLogAndAssertEvents(logReader, WORK_COUNT, MSG_SIZE);
        newLog.close();

        final int newIndexSize = newLog.getLogBlockIndex().size();
        assertThat(newIndexSize).isEqualTo(0);
    }

    @Test
    public void shouldRecoverIndexFromSnapshot() throws InterruptedException, ExecutionException
    {
        // given open log stream
        final AtomicBoolean isLastLogEntry = new AtomicBoolean(false);
        final LogStream log = logStreamBuilder
            .snapshotPolicy((pos) -> isLastLogEntry.getAndSet(false))
            .build();
        log.setCommitPosition(Long.MAX_VALUE);
        log.open();
        writeLogEvents(log, WORK_COUNT / 2, MSG_SIZE, 0);
        waitUntilWrittenKey(log, WORK_COUNT / 2);
        final int indexSize = log.getLogBlockIndex().size();
        isLastLogEntry.set(true);

        // on next block index creation a snapshot is created
        writeLogEvents(log, WORK_COUNT / 2, MSG_SIZE, WORK_COUNT / 2);
        waitUntilWrittenKey(log, WORK_COUNT);
        log.close();
        final int endIndexSize = log.getLogBlockIndex().size();
        assertThat(endIndexSize).isGreaterThan(indexSize);

        // when new log stream is opened
        final LogStream newLog = getLogStreamBuilder()
            .logStreamControllerDisabled(true)
            .build();
        newLog.setCommitPosition(Long.MAX_VALUE);
        newLog.open();

        // then block index is recovered
        final int recoveredIndexSize = newLog.getLogBlockIndex().size();
        assertThat(recoveredIndexSize).isGreaterThan(indexSize);

        final LogStreamReader logReader = new BufferedLogStreamReader(newLog);
        LogIntegrationTestUtil.readLogAndAssertEvents(logReader, WORK_COUNT, MSG_SIZE);
        newLog.close();

        // and after read events more block indices are created
        final int newIndexSize = newLog.getLogBlockIndex().size();
        assertThat(newIndexSize).isGreaterThan(recoveredIndexSize);
    }

    @Test
    public void shouldRecoverIndexFromSnapshotButNotCreateMoreIndices() throws InterruptedException, ExecutionException
    {
        // given open log stream
        final AtomicBoolean isLastLogEntry = new AtomicBoolean(false);
        final LogStream log = logStreamBuilder
            .snapshotPolicy((pos) -> isLastLogEntry.getAndSet(false))
            .build();
        log.setCommitPosition(Long.MAX_VALUE);
        log.open();
        writeLogEvents(log, WORK_COUNT / 2, MSG_SIZE, 0);
        waitUntilWrittenKey(log, WORK_COUNT / 2);
        final int indexSize = log.getLogBlockIndex().size();
        isLastLogEntry.set(true);

        // on next block index creation a snapshot is created
        writeLogEvents(log, WORK_COUNT / 2, MSG_SIZE, WORK_COUNT / 2);
        waitUntilWrittenKey(log, WORK_COUNT);
        log.close();
        final int endIndexSize = log.getLogBlockIndex().size();
        assertThat(endIndexSize).isGreaterThan(indexSize);

        // when new log stream is opened
        final LogStream newLog = LogStreams.createFsLogStream(TOPIC_NAME, PARTITION_ID)
            .logRootPath(logPath)
            .deleteOnClose(true)
            .logSegmentSize(LOG_SEGMENT_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE)
            .readBlockSize(INDEX_BLOCK_SIZE)
            .taskScheduler(taskScheduler)
            .logStreamControllerDisabled(true)
            .build();
        newLog.open();

        // then block index is recovered
        final int recoveredIndexSize = newLog.getLogBlockIndex().size();
        assertThat(recoveredIndexSize).isEqualTo(indexSize + 1);

        // but no further indices are created
        final LogStreamReader logReader = new BufferedLogStreamReader(newLog, true);
        LogIntegrationTestUtil.readLogAndAssertEvents(logReader, WORK_COUNT, MSG_SIZE);
        newLog.close();

        final int newIndexSize = newLog.getLogBlockIndex().size();
        assertThat(newIndexSize).isEqualTo(recoveredIndexSize);
    }

    @Test
    public void shouldRecoverIndexFromPeriodicallyCreatedSnapshot() throws InterruptedException, ExecutionException
    {
        // given open log stream with periodically created snapshot
        final int snapshotInterval = 10;
        final AtomicInteger snapshotCount = new AtomicInteger(0);
        final LogStream log = logStreamBuilder
            .snapshotPolicy(pos -> (snapshotCount.incrementAndGet() % snapshotInterval) == 0)
            .build();
        log.setCommitPosition(Long.MAX_VALUE);
        log.open();
        writeLogEvents(log, WORK_COUNT, MSG_SIZE, 0);
        waitUntilWrittenKey(log, WORK_COUNT);
        log.close();

        final LogStream newLog = LogStreams.createFsLogStream(TOPIC_NAME, PARTITION_ID)
            .logRootPath(logPath)
            .deleteOnClose(true)
            .logSegmentSize(LOG_SEGMENT_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE)
            .readBlockSize(INDEX_BLOCK_SIZE)
            .taskScheduler(taskScheduler)
            .logStreamControllerDisabled(true)
            .build();
        newLog.open();

        // then block index is recovered
        final int recoveredIndexSize = newLog.getLogBlockIndex().size();
        assertThat(recoveredIndexSize).isGreaterThan(0);

        final int indexSize = log.getLogBlockIndex().size();
        final int calculatesIndexSize = calculateIndexSize(WORK_COUNT);
        assertThat(indexSize).isGreaterThan(calculatesIndexSize);
        readLogAndAssertEvents(WORK_COUNT, calculatesIndexSize);
    }

    @Test
    public void shouldRecoverEventPosition() throws InterruptedException, ExecutionException
    {
        final LogStream log = logStreamBuilder.build();
        log.setCommitPosition(Long.MAX_VALUE);
        log.open();

        // write events
        writeLogEvents(log, 2 * WORK_COUNT_PER_BLOCK_IDX, MSG_SIZE, 0);
        waitUntilWrittenKey(log, 2 * WORK_COUNT_PER_BLOCK_IDX);

        log.close();
        int indexSize = log.getLogBlockIndex().size();
        assertThat(indexSize).isGreaterThan(0);

        // re-open the log
        final LogStream newLog = getLogStreamBuilder().build();
        newLog.setCommitPosition(Long.MAX_VALUE);
        newLog.open();

        // check if new log creates indices
        // perhaps not equal since he has to process all events
        final int newIndexSize = newLog.getLogBlockIndex().size();
        assertThat(newIndexSize).isGreaterThan(0);
        assertThat(indexSize).isLessThanOrEqualTo(newIndexSize);

        // write more events
        writeLogEvents(newLog, 2 * WORK_COUNT_PER_BLOCK_IDX, MSG_SIZE, 2 * WORK_COUNT_PER_BLOCK_IDX);
        waitUntilWrittenKey(newLog, 4 * WORK_COUNT_PER_BLOCK_IDX);

        newLog.close();
        indexSize = newLog.getLogBlockIndex().size();
        final int calculatesIndexSize = calculateIndexSize(4 * WORK_COUNT_PER_BLOCK_IDX);
        assertThat(indexSize).isGreaterThanOrEqualTo(calculatesIndexSize);

        // assert that the event position is recovered after re-open and continues after the last event
        readLogAndAssertEvents(4 * WORK_COUNT_PER_BLOCK_IDX, calculatesIndexSize);
    }

    @Test
    public void shouldResumeLogStream() throws InterruptedException, ExecutionException
    {
        final LogStream logStream = logStreamBuilder.build();
        logStream.setCommitPosition(Long.MAX_VALUE);
        logStream.open();
        // write events
        writeLogEvents(logStream, WORK_COUNT_PER_BLOCK_IDX / 10, MSG_SIZE, 0);
        waitUntilWrittenKey(logStream, WORK_COUNT_PER_BLOCK_IDX / 10);

        logStream.close();
        int indexSize = logStream.getLogBlockIndex().size();
        assertThat(indexSize).isEqualTo(0);

        // resume the log
        logStream.open();

        // write more events
        writeLogEvents(logStream, 2 * WORK_COUNT_PER_BLOCK_IDX, MSG_SIZE, WORK_COUNT_PER_BLOCK_IDX / 10);
        waitUntilWrittenKey(logStream, 2 * WORK_COUNT_PER_BLOCK_IDX + (WORK_COUNT_PER_BLOCK_IDX / 10));

        logStream.close();

        // after resume an index was written
        final int calculatesIndexSize = calculateIndexSize(2 * WORK_COUNT_PER_BLOCK_IDX + (WORK_COUNT_PER_BLOCK_IDX / 10));
        assertThat(logStream.getLogBlockIndex().size()).isGreaterThan(indexSize);
        indexSize = logStream.getLogBlockIndex().size();
        assertThat(indexSize).isGreaterThanOrEqualTo(calculatesIndexSize);

        readLogAndAssertEvents(2 * WORK_COUNT_PER_BLOCK_IDX + (WORK_COUNT_PER_BLOCK_IDX / 10), calculatesIndexSize);
    }

    protected void readLogAndAssertEvents(int workCount, int indexSize)
    {
        final LogStream newLog = getLogStreamBuilder()
            .deleteOnClose(true)
            .logStreamControllerDisabled(true)
            .build();
        newLog.setCommitPosition(Long.MAX_VALUE);
        newLog.open();

        final LogStreamReader logReader = new BufferedLogStreamReader(newLog);

        LogIntegrationTestUtil.readLogAndAssertEvents(logReader, workCount, MSG_SIZE);

        newLog.close();

        final int newIndexSize = newLog.getLogBlockIndex().size();
        assertThat(newIndexSize).isGreaterThanOrEqualTo(indexSize);
    }

    private int calculateIndexSize(int workCount)
    {
        // WORK (count * message size) / index block size = is equal to the count of
        // block indices for each block which has the size of index block size.
        // Sometimes the index is created for larger blocks. If events are not read complete
        // they are truncated, for this case the block will not reach the index block size. In the next read step
        // the event will be read complete but also other events which means the block is now larger then the index block size.
        //
        // The count of created indices should be greater than the half of the optimal index count.
        //
        return (int) (Math.floor(workCount * MSG_SIZE) / INDEX_BLOCK_SIZE) / 2;
    }
}
