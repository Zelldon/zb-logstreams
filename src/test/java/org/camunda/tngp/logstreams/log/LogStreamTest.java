package org.camunda.tngp.logstreams.log;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.camunda.tngp.dispatcher.Dispatcher;
import org.camunda.tngp.dispatcher.Subscription;
import org.camunda.tngp.logstreams.fs.FsLogStreamBuilder;
import org.camunda.tngp.logstreams.impl.LogBlockIndexController;
import org.camunda.tngp.logstreams.impl.LogStreamController;
import org.camunda.tngp.logstreams.impl.LogStreamImpl;
import org.camunda.tngp.logstreams.impl.log.index.LogBlockIndex;
import org.camunda.tngp.logstreams.spi.SnapshotPolicy;
import org.camunda.tngp.logstreams.spi.SnapshotStorage;
import org.camunda.tngp.logstreams.spi.SnapshotWriter;
import org.camunda.tngp.util.agent.AgentRunnerService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.join;
import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor.alignedLength;
import static org.camunda.tngp.logstreams.impl.LogEntryDescriptor.HEADER_BLOCK_LENGTH;
import static org.camunda.tngp.logstreams.log.LogStream.MAX_TOPIC_NAME_LENGTH;
import static org.camunda.tngp.logstreams.log.LogStreamUtil.INVALID_ADDRESS;
import static org.camunda.tngp.logstreams.log.LogTestUtil.*;
import static org.camunda.tngp.logstreams.log.MockLogStorage.newLogEntry;
import static org.camunda.tngp.util.StringUtil.getBytes;
import static org.camunda.tngp.util.buffer.BufferUtil.wrapString;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class LogStreamTest
{
    private static final int MAX_APPEND_BLOCK_SIZE = 1024 * 1024 * 6;
    private static final int INDEX_BLOCK_SIZE = 1024 * 1024 * 2;

    private static final long TRUNCATE_START_ADDRESS = 12345L;
    private static final int TRUNCATE_POSITION = 101;
    private static final int EVENT_SIZE = alignedLength(HEADER_BLOCK_LENGTH);

    public LogStream logStream;

    @Mock
    LogStreamImpl.LogStreamBuilder mockLogStreamBuilder;

    @Mock
    private AgentRunnerService mockAgentRunnerService;
    @Mock
    private AgentRunnerService mockConductorAgentRunnerService;

    @Mock
    private Dispatcher mockWriteBuffer;
    @Mock
    private Subscription mockWriteBufferSubscription;
    @Mock
    private Agent mockWriteBufferConductorAgent;

    @Mock
    private LogBlockIndex mockBlockIndex;

    private MockLogStorage mockLogStorage;

    @Mock
    private SnapshotStorage mockSnapshotStorage;
    @Mock
    private SnapshotWriter mockSnapshotWriter;
    @Mock
    private SnapshotPolicy mockSnapshotPolicy;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void init() throws Exception
    {
        MockitoAnnotations.initMocks(this);

        this.mockLogStorage = new MockLogStorage();

        final FsLogStreamBuilder builder = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID);
        builder.agentRunnerService(mockAgentRunnerService)
            .writeBufferAgentRunnerService(mockConductorAgentRunnerService)
            .writeBuffer(mockWriteBuffer)
            .logStorage(mockLogStorage.getMock())
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .maxAppendBlockSize(MAX_APPEND_BLOCK_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE);

        when(mockBlockIndex.lookupBlockAddress(anyLong())).thenReturn(LOG_ADDRESS);
        when(mockWriteBuffer.getSubscriptionByName("log-appender")).thenReturn(mockWriteBufferSubscription);
        when(mockWriteBuffer.getConductorAgent()).thenReturn(mockWriteBufferConductorAgent);
        when(mockSnapshotStorage.createSnapshot(anyString(), anyLong())).thenReturn(mockSnapshotWriter);

        logStream = builder.build();
    }

    @Test
    public void shouldFailWithToLongTopicName()
    {
        // given
        final DirectBuffer topicName = wrapString(join("", Collections.nCopies(MAX_TOPIC_NAME_LENGTH + 1, "f")));

        final FsLogStreamBuilder builder = new FsLogStreamBuilder(topicName, PARTITION_ID)
            .agentRunnerService(mockAgentRunnerService)
            .writeBufferAgentRunnerService(mockConductorAgentRunnerService)
            .writeBuffer(mockWriteBuffer)
            .logStorage(mockLogStorage.getMock())
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .maxAppendBlockSize(MAX_APPEND_BLOCK_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE);

        // expect exception
        thrown.expect(RuntimeException.class);
        thrown.expectMessage(String.format("Topic name exceeds max length (%d > %d bytes)", topicName.capacity(), MAX_TOPIC_NAME_LENGTH));

        // when
        builder.build();
    }

    @Test
    public void shouldInitCorrectly()
    {
        // when log stream is created with builder

        // then log stream contains
        // log storage
        assertNotNull(logStream.getLogStorage());
        assertEquals(logStream.getLogStorage(), mockLogStorage.getMock());

        // block index
        assertNotNull(logStream.getLogBlockIndex());
        assertEquals(logStream.getLogBlockIndex(), mockBlockIndex);

        // and dispatcher
        assertNotNull(logStream.getWriteBuffer());
        assertEquals(logStream.getWriteBuffer(), mockWriteBuffer);

        // both controllers are created
        assertNotNull(logStream.getLogBlockIndexController());
        assertNotNull(logStream.getLogStreamController());
    }

    @Test
    public void shouldInitWithoutLogStreamController()
    {
        // when log stream is created with builder and without flag is set
        final FsLogStreamBuilder builder = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID);
        final LogStream stream = builder.agentRunnerService(mockAgentRunnerService)
            .logStreamControllerDisabled(true)
            .logStorage(mockLogStorage.getMock())
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .maxAppendBlockSize(MAX_APPEND_BLOCK_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE).build();

        // then log stream contains
        // log storage
        assertNotNull(stream.getLogStorage());
        assertEquals(stream.getLogStorage(), mockLogStorage.getMock());

        // block index
        assertNotNull(stream.getLogBlockIndex());
        assertEquals(stream.getLogBlockIndex(), mockBlockIndex);

        // and no dispatcher
        assertNull(stream.getWriteBuffer());

        // only  log block index controller is created
        assertNotNull(stream.getLogBlockIndexController());
        assertNull(stream.getLogStreamController());
    }

    @Test
    public void shouldOpenBothController()
    {
        // given log stream
        when(logStream.getLogStorage().isOpen()).thenReturn(true);

        // when log stream is open
        final CompletableFuture<Void> completableFuture = logStream.openAsync();
        final LogBlockIndexController logBlockIndexController = logStream.getLogBlockIndexController();
        logBlockIndexController.doWork();
        final LogStreamController logStreamController = logStream.getLogStreamController();
        logStreamController.doWork();

        // then
        assertTrue(completableFuture.isDone());
        // log block index is opened and runs now
        assertTrue(logBlockIndexController.isRunning());

        // log stream controller is opened and runs now
        assertTrue(logStreamController.isRunning());

        // and logStorage is opened
        assertTrue(logStream.getLogStorage().isOpen());
    }

    @Test
    public void shouldOpenLogBlockIndexControllerOnly()
    {
        // given log stream with without flag
        when(logStream.getLogStorage().isOpen()).thenReturn(true);
        final FsLogStreamBuilder builder = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID);
        final LogStream stream = builder.agentRunnerService(mockAgentRunnerService)
            .logStreamControllerDisabled(true)
            .logStorage(mockLogStorage.getMock())
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .maxAppendBlockSize(MAX_APPEND_BLOCK_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE).build();

        // when log stream is open
        final CompletableFuture<Void> completableFuture = stream.openAsync();
        final LogBlockIndexController logBlockIndexController = stream.getLogBlockIndexController();
        logBlockIndexController.doWork();
        final LogStreamController logStreamController = stream.getLogStreamController();

        // then
        assertTrue(completableFuture.isDone());
        // log block index is opened and runs now
        assertTrue(logBlockIndexController.isRunning());

        // log stream controller is null
        assertNull(logStreamController);

        // and logStorage is opened
        assertTrue(stream.getLogStorage().isOpen());
    }

    @Test
    public void shouldStopLogStreamController()
    {
        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.complete(null);
        when(mockWriteBuffer.closeAsync()).thenReturn(completableFuture);

        // given open log stream
        logStream.openAsync();
        final LogBlockIndexController logBlockIndexController = logStream.getLogBlockIndexController();
        logBlockIndexController.doWork();
        final LogStreamController logStreamController = logStream.getLogStreamController();
        logStreamController.doWork();

        // when log streaming is stopped
        logStream.closeLogStreamController();
        logStreamController.doWork(); // closing
        logStreamController.doWork(); // close

        // then
        // log stream controller has stop running and reference is null
        assertFalse(logStreamController.isRunning());
        assertNull(logStream.getLogStreamController());

        // dispatcher is null as well
        assertNull(logStream.getWriteBuffer());
//        verify(mockWriteBuffer).closeAsync();

        // agent and controller is removed from agent runner's
        verify(mockConductorAgentRunnerService).remove(mockWriteBufferConductorAgent);
        verify(mockAgentRunnerService).remove(logStreamController);
    }

    @Test
    public void shouldStopAndStartLogStreamController()
    {
        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.complete(null);
        when(mockWriteBuffer.closeAsync()).thenReturn(completableFuture);

        // set up block index and log storage
        when(mockBlockIndex.size()).thenReturn(1);
        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.lookupBlockAddress(Long.MAX_VALUE)).thenReturn(10L);

        mockLogStorage.add(newLogEntry()
            .address(10)
            .position(1)
            .key(2)
            .sourceEventLogStreamId(3)
            .sourceEventPosition(4L)
            .producerId(5)
            .value(getBytes("event")));

        // given open log stream with stopped log stream controller
        logStream.openAsync();
        logStream.getLogBlockIndexController().doWork();
        LogStreamController logStreamController = logStream.getLogStreamController();
        logStreamController.doWork();

        logStream.closeLogStreamController();
        logStreamController.doWork(); // closing
        logStreamController.doWork(); // close

        // when log streaming is started
        logStream.openLogStreamController();
        logStreamController = logStream.getLogStreamController();

        // then
        // log stream controller has been set
        assertNotNull(logStreamController);
        logStreamController.doWork();
        // is running
        assertTrue(logStreamController.isRunning());

        // dispatcher is initialized
        assertNotNull(logStream.getWriteBuffer());

        // verify usage of agent runner service, only one times with mock because after re-opening new agent is used
        verify(mockConductorAgentRunnerService, times(2)).run(any(Agent.class));
        verify(mockConductorAgentRunnerService).run(mockWriteBufferConductorAgent);
        verify(mockAgentRunnerService).run(logStreamController);
    }

    @Test
    public void shouldStopAndStartLogStreamControllerWithDifferentAgentRunners()
    {
        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.complete(null);
        when(mockWriteBuffer.closeAsync()).thenReturn(completableFuture);

        // set up block index and log storage
        when(mockBlockIndex.size()).thenReturn(1);
        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.lookupBlockAddress(Long.MAX_VALUE)).thenReturn(10L);

        mockLogStorage.add(newLogEntry()
            .address(10)
            .position(1)
            .key(2)
            .sourceEventLogStreamId(3)
            .sourceEventPosition(4L)
            .producerId(5)
            .value(getBytes("event")));
        final AgentRunnerService secondAgentRunnerService = mock(AgentRunnerService.class);

        // given open log stream with stopped log stream controller
        logStream.openAsync();
        logStream.getLogBlockIndexController().doWork();
        LogStreamController logStreamController = logStream.getLogStreamController();
        logStreamController.doWork();

        logStream.closeLogStreamController();
        logStreamController.doWork(); // closing
        logStreamController.doWork(); // close

        // when log streaming is started
        logStream.openLogStreamController(secondAgentRunnerService);
        logStreamController = logStream.getLogStreamController();

        // then
        // log stream controller has been set
        assertNotNull(logStreamController);
        logStreamController.doWork();
        // is running
        assertTrue(logStreamController.isRunning());

        // dispatcher is initialized
        assertNotNull(logStream.getWriteBuffer());

        // verify usage of agent runner service
        verify(mockConductorAgentRunnerService).run(mockWriteBufferConductorAgent);
        verify(secondAgentRunnerService).run(any(Agent.class));
        verify(mockAgentRunnerService).run(logStreamController);
    }

    @Test
    public void shouldStartLogStreamController()
    {
        // set up block index and log storage
        when(mockBlockIndex.size()).thenReturn(1);
        when(mockBlockIndex.getLogPosition(0)).thenReturn(1L);
        when(mockBlockIndex.lookupBlockAddress(Long.MAX_VALUE)).thenReturn(10L);

        mockLogStorage.add(newLogEntry()
            .address(10)
            .position(1)
            .key(2)
            .sourceEventLogStreamId(3)
            .sourceEventPosition(4L)
            .producerId(5)
            .value(getBytes("event")));

        // given log stream with without flag
        final FsLogStreamBuilder builder = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID);
        final LogStream stream = builder.agentRunnerService(mockAgentRunnerService)
            .logStreamControllerDisabled(true)
            .logStorage(mockLogStorage.getMock())
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .maxAppendBlockSize(MAX_APPEND_BLOCK_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE).build();

        // when log streaming is started
        stream.openLogStreamController(mockConductorAgentRunnerService);
        final LogStreamController logStreamController = stream.getLogStreamController();

        // then
        // log stream controller has been set
        assertNotNull(logStreamController);
        logStreamController.doWork();
        // is running
        assertTrue(logStreamController.isRunning());

        // dispatcher is initialized
        final Dispatcher writeBuffer = stream.getWriteBuffer();
        assertNotNull(writeBuffer);

        // verify usage of agent runner service
        verify(mockConductorAgentRunnerService).run(writeBuffer.getConductorAgent());
        verify(mockAgentRunnerService).run(logStreamController);
    }

    @Test
    public void shouldCloseBothController()
    {
        // given open log stream
        logStream.openAsync();
        final LogBlockIndexController logBlockIndexController = logStream.getLogBlockIndexController();
        logBlockIndexController.doWork();
        final LogStreamController logStreamController = logStream.getLogStreamController();
        logStreamController.doWork();

        // when log stream is closed
        final CompletableFuture<Void> completableFuture = logStream.closeAsync();
        logBlockIndexController.doWork(); //closing
        logBlockIndexController.doWork(); //close

        logStreamController.doWork(); // closing
        logStreamController.doWork(); // close

        // then future is complete
        assertTrue(completableFuture.isDone());

        // controllers are not running
        assertFalse(logBlockIndexController.isRunning());
        assertFalse(logStreamController.isRunning());

        // and log storage was closed
        verify(mockLogStorage.getMock()).close();
    }

    @Test
    public void shouldCloseLogBlockIndexController()
    {
        // given open log stream without log stream controller
        final FsLogStreamBuilder builder = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID);
        final LogStream stream = builder.agentRunnerService(mockAgentRunnerService)
            .logStreamControllerDisabled(true)
            .logStorage(mockLogStorage.getMock())
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .maxAppendBlockSize(MAX_APPEND_BLOCK_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE).build();
        stream.openAsync();

        final LogBlockIndexController logBlockIndexController = stream.getLogBlockIndexController();
        logBlockIndexController.doWork();

        // when log stream is closed
        final CompletableFuture<Void> completableFuture = stream.closeAsync();
        logBlockIndexController.doWork(); //closing
        logBlockIndexController.doWork(); //close

        // then future is complete
        assertTrue(completableFuture.isDone());

        // controllers are not running
        assertFalse(logBlockIndexController.isRunning());

        // and log storage was closed
        verify(mockLogStorage.getMock()).close();
    }

    @Test
    public void shouldOpenBothClosedController()
    {
        // given open->close log stream
        logStream.openAsync();
        final LogBlockIndexController logBlockIndexController = logStream.getLogBlockIndexController();
        logBlockIndexController.doWork();
        final LogStreamController logStreamController = logStream.getLogStreamController();
        logStreamController.doWork();

        logStream.closeAsync();
        logBlockIndexController.doWork(); //closing
        logBlockIndexController.doWork(); //close

        logStreamController.doWork(); // closing
        logStreamController.doWork(); // close

        // when open log stream again
        final CompletableFuture<Void> completableFuture = logStream.openAsync();
        logBlockIndexController.doWork(); //opening
        logStreamController.doWork(); // opening

        // then controllers run again
        assertTrue(completableFuture.isDone());
        assertTrue(logBlockIndexController.isRunning());
        assertTrue(logStreamController.isRunning());
    }

    @Test
    public void shouldOpenClosedLogBlockIndexController()
    {
        // given open->close log stream without log stream controller
        final FsLogStreamBuilder builder = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID);
        final LogStream stream = builder.agentRunnerService(mockAgentRunnerService)
            .logStreamControllerDisabled(true)
            .logStorage(mockLogStorage.getMock())
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .maxAppendBlockSize(MAX_APPEND_BLOCK_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE).build();
        stream.openAsync();
        final LogBlockIndexController logBlockIndexController = stream.getLogBlockIndexController();
        logBlockIndexController.doWork();

        stream.closeAsync();
        logBlockIndexController.doWork(); //closing
        logBlockIndexController.doWork(); //close

        // when open log stream again
        final CompletableFuture<Void> completableFuture = stream.openAsync();
        logBlockIndexController.doWork(); //opening

        // then controllers run again
        assertTrue(completableFuture.isDone());
        assertTrue(logBlockIndexController.isRunning());
    }

    @Test
    public void shouldThrowExceptionForTruncationWithLogStreamController()
    {
        // given open log stream
        logStream.openAsync();
        final LogBlockIndexController logBlockIndexController = logStream.getLogBlockIndexController();
        logBlockIndexController.doWork();
        final LogStreamController logStreamController = logStream.getLogStreamController();
        logStreamController.doWork();

        // expect exception
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(LogStreamImpl.EXCEPTION_MSG_TRUNCATE_AND_LOG_STREAM_CTRL_IN_PARALLEL);

        // when truncate is called
        logStream.truncate(0);
    }

    @Test
    public void shouldThrowExceptionForTruncationOfAlreadyCommittedPosition()
    {
        final LogStream stream = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID)
            .agentRunnerService(mockAgentRunnerService)
            .logStorage(mockLogStorage.getMock())
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .logStreamControllerDisabled(true)
            .indexBlockSize(INDEX_BLOCK_SIZE)
            .build();

        // given open log stream and committed position
        stream.openAsync();
        stream.setCommitPosition(TRUNCATE_POSITION);

        // expect exception
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(LogStreamImpl.EXCEPTION_MSG_TRUNCATE_COMMITTED_POSITION);

        // when truncate is called
        stream.truncate(TRUNCATE_POSITION);
    }

    @Test
    public void shouldTruncateLogStorage()
    {
        final MockLogStorage storage = new MockLogStorage();
        storage.add(newLogEntry().partlyRead());

        final LogStream stream = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID)
            .agentRunnerService(mockAgentRunnerService)
            .logStorage(storage.getMock())
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .logStreamControllerDisabled(true)
            .indexBlockSize(INDEX_BLOCK_SIZE)
            .build();

        // given open log stream and open block index controller
        stream.openAsync();
        stream.getLogBlockIndexController().doWork();

        // when
        final CompletableFuture<Void> truncateFuture = stream.truncate(TRUNCATE_POSITION);

        // then
        verify(storage.getMock()).truncate(EVENT_SIZE * TRUNCATE_POSITION);
        assertThat(truncateFuture.isDone()).isFalse();
        assertThat(stream.getLogBlockIndexController().getNextAddress()).isNotEqualTo(INVALID_ADDRESS);

        // when
        stream.getLogBlockIndexController().doWork();

        // then
        assertThat(truncateFuture.isDone()).isTrue();
        assertThat(stream.getLogBlockIndexController().getNextAddress()).isEqualTo(INVALID_ADDRESS);
    }

    @Test
    public void shouldTruncateLogStorageWithCommittedPosition()
    {
        final MockLogStorage storage = new MockLogStorage();
        storage.add(newLogEntry().partlyRead());

        final LogStream stream = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID)
            .agentRunnerService(mockAgentRunnerService)
            .logStorage(storage.getMock())
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .logStreamControllerDisabled(true)
            .indexBlockSize(INDEX_BLOCK_SIZE)
            .build();

        // given open log stream and open block index controller
        // and committed position
        stream.setCommitPosition(TRUNCATE_POSITION - 1);
        stream.openAsync();
        stream.getLogBlockIndexController().doWork();

        // when
        final CompletableFuture<Void> truncateFuture = stream.truncate(TRUNCATE_POSITION);

        // then
        verify(storage.getMock()).truncate(EVENT_SIZE * TRUNCATE_POSITION);
        assertThat(truncateFuture.isDone()).isFalse();
        assertThat(stream.getLogBlockIndexController().getNextAddress()).isNotEqualTo(INVALID_ADDRESS);

        // when
        stream.getLogBlockIndexController().doWork();

        // then
        assertThat(truncateFuture.isDone()).isTrue();
        assertThat(stream.getLogBlockIndexController().getNextAddress()).isEqualTo(INVALID_ADDRESS);
    }

    @Test
    public void shouldTruncateLogStorageForExistingBlockIndex()
    {
        final MockLogStorage storage = new MockLogStorage();
        storage.add(newLogEntry().partlyRead());

        final LogStream stream = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID)
            .agentRunnerService(mockAgentRunnerService)
            .logStorage(storage.getMock())
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .logStreamControllerDisabled(true)
            .indexBlockSize(INDEX_BLOCK_SIZE)
            .build();

        when(mockBlockIndex.lookupBlockAddress(TRUNCATE_POSITION)).thenReturn(TRUNCATE_START_ADDRESS);
        when(mockBlockIndex.size()).thenReturn(1);

        // given open log stream and open block index controller
        stream.openAsync();
        stream.getLogBlockIndexController().doWork();

        // when
        final CompletableFuture<Void> truncateFuture = stream.truncate(TRUNCATE_POSITION);

        // then
        verify(storage.getMock()).truncate(TRUNCATE_START_ADDRESS + EVENT_SIZE * TRUNCATE_POSITION);
        assertThat(truncateFuture.isDone()).isFalse();
        assertThat(stream.getLogBlockIndexController().getNextAddress()).isNotEqualTo(INVALID_ADDRESS);

        // when
        stream.getLogBlockIndexController().doWork();

        // then
        assertThat(truncateFuture.isDone()).isTrue();
        assertThat(stream.getLogBlockIndexController().getNextAddress()).isEqualTo(INVALID_ADDRESS);
    }

    @Test
    public void shouldTruncateLogStorageForExistingBlockIndexAndCommittedPosition()
    {
        final MockLogStorage storage = new MockLogStorage();
        storage.add(newLogEntry().partlyRead());

        final LogStream stream = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID)
            .agentRunnerService(mockAgentRunnerService)
            .logStorage(storage.getMock())
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .logStreamControllerDisabled(true)
            .indexBlockSize(INDEX_BLOCK_SIZE)
            .build();

        when(mockBlockIndex.lookupBlockAddress(TRUNCATE_POSITION)).thenReturn(TRUNCATE_START_ADDRESS);
        when(mockBlockIndex.size()).thenReturn(1);

        // given open log stream and open block index controller
        // and committed position
        stream.setCommitPosition(TRUNCATE_POSITION - 1);
        stream.openAsync();
        stream.getLogBlockIndexController().doWork();

        // when
        final CompletableFuture<Void> truncateFuture = stream.truncate(TRUNCATE_POSITION);

        // then
        verify(storage.getMock()).truncate(TRUNCATE_START_ADDRESS + EVENT_SIZE * TRUNCATE_POSITION);
        assertThat(truncateFuture.isDone()).isFalse();
        assertThat(stream.getLogBlockIndexController().getNextAddress()).isNotEqualTo(INVALID_ADDRESS);

        // when
        stream.getLogBlockIndexController().doWork();

        // then
        assertThat(truncateFuture.isDone()).isTrue();
        assertThat(stream.getLogBlockIndexController().getNextAddress()).isEqualTo(INVALID_ADDRESS);
    }

    @Test
    public void shouldNotTruncateIfPositionWasNotFound()
    {
        // given
        final MockLogStorage storage = new MockLogStorage();
        storage.add(newLogEntry().maxPosition(TRUNCATE_POSITION - 1).partlyRead());
        final LogStream stream = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID)
            .agentRunnerService(mockAgentRunnerService)
            .logStorage(storage.getMock())
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .logStreamControllerDisabled(true)
            .indexBlockSize(INDEX_BLOCK_SIZE)
            .build();

        when(mockBlockIndex.lookupBlockAddress(TRUNCATE_POSITION)).thenReturn(LOG_ADDRESS);
        when(mockBlockIndex.size()).thenReturn(1);

        // when truncate is called
        final CompletableFuture<Void> truncateFuture = stream.truncate(TRUNCATE_POSITION);

        // then truncate was completed exceptionally
        assertThat(truncateFuture.isCompletedExceptionally()).isTrue();
        assertThat(truncateFuture).hasFailedWithThrowableThat().hasMessage("Truncation failed! Position " + TRUNCATE_POSITION + " was not found.");
    }
}
