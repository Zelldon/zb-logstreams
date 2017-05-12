package org.camunda.tngp.logstreams.log;

import org.agrona.concurrent.status.AtomicLongPosition;
import org.camunda.tngp.logstreams.fs.FsLogStreamBuilder;
import org.camunda.tngp.logstreams.impl.LogBlockIndexController;
import org.camunda.tngp.logstreams.impl.log.index.LogBlockIndex;
import org.camunda.tngp.logstreams.spi.*;
import org.camunda.tngp.util.agent.AgentRunnerService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.tngp.logstreams.log.LogStreamUtil.INVALID_ADDRESS;
import static org.camunda.tngp.logstreams.log.LogTestUtil.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author Christopher Zell <christopher.zell@camunda.com>
 */
public class LogBlockIndexControllerTest
{
    private static final int INDEX_BLOCK_SIZE = 1024 * 2;
    private static final int READ_BLOCK_SIZE = 1024;

    private LogBlockIndexController blockIdxController;

    @Mock
    private AgentRunnerService mockAgentRunnerService;

    @Mock
    private LogBlockIndex mockBlockIndex;

    @Mock
    private LogStorage mockLogStorage;
    private final AtomicLongPosition commitPosition = new AtomicLongPosition();

    @Mock
    private SnapshotStorage mockSnapshotStorage;
    @Mock
    private ReadableSnapshot mockSnapshot;
    @Mock
    private SnapshotWriter mockSnapshotWriter;
    @Mock
    private SnapshotPolicy mockSnapshotPolicy;

    @Before
    public void init() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        commitPosition.setOrdered(Long.MAX_VALUE);

        final FsLogStreamBuilder builder = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID)
            .agentRunnerService(mockAgentRunnerService)
            .logStreamControllerDisabled(true)
            .logStorage(mockLogStorage)
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .readBlockSize(READ_BLOCK_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE);

        when(mockLogStorage.getFirstBlockAddress()).thenReturn(LOG_ADDRESS);
        when(mockBlockIndex.lookupBlockAddress(anyLong())).thenReturn(LOG_ADDRESS);
        when(mockSnapshotStorage.createSnapshot(anyString(), anyLong())).thenReturn(mockSnapshotWriter);

        blockIdxController = new LogBlockIndexController(builder, commitPosition);

        blockIdxController.doWork();
    }

    @Test
    public void shouldRecoverBlockIndexFromLogStorageWhileOpening() throws Exception
    {
        when(mockSnapshotStorage.getLastSnapshot(LOG_NAME)).thenReturn(null);

        final CompletableFuture<Void> future = blockIdxController.openAsync();

        blockIdxController.doWork();

        assertThat(future).isCompleted();

        verify(mockLogStorage).getFirstBlockAddress();
        assertThat(blockIdxController.getNextAddress()).isEqualTo(LOG_ADDRESS);
    }

    @Test
    public void shouldRecoverBlockIndexFromSnapshotWhileOpening() throws Exception
    {
        when(mockSnapshotStorage.getLastSnapshot(LOG_NAME)).thenReturn(mockSnapshot);
        when(mockSnapshot.getPosition()).thenReturn(100L);

        final CompletableFuture<Void> future = blockIdxController.openAsync();

        blockIdxController.doWork();

        assertThat(future).isCompleted();

        verify(mockSnapshot).recoverFromSnapshot(mockBlockIndex);

        verify(mockBlockIndex).lookupBlockAddress(mockSnapshot.getPosition());
    }

    @Test
    public void shouldOpenIfFailToRecoverBlockIndexFromSnapshot() throws Exception
    {
        when(mockSnapshotStorage.getLastSnapshot(LOG_NAME)).thenReturn(mockSnapshot);
        when(mockSnapshot.getPosition()).thenReturn(100L);

        doThrow(new RuntimeException()).when(mockSnapshot).recoverFromSnapshot(any(SnapshotSupport.class));

        final CompletableFuture<Void> future = blockIdxController.openAsync();

        // when recovery failed
        blockIdxController.doWork();

        // then we open either way
        assertThat(future).isCompleted();
        assertThat(blockIdxController.isOpen()).isTrue();
    }

    @Test
    public void shouldCreateSnapshot() throws Exception
    {
        when(mockBlockIndex.getLogPosition(anyInt())).thenReturn(LOG_POSITION);

        final ByteBuffer buffer = ByteBuffer.allocate(READ_BLOCK_SIZE);
        when(mockLogStorage.read(eq(buffer), eq(LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(READ_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        when(mockLogStorage.read(any(ByteBuffer.class), eq(READ_BLOCK_SIZE + LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(2 * READ_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        when(mockSnapshotPolicy.apply(LOG_POSITION)).thenReturn(true);

        blockIdxController.openAsync();
        // -> opening
        blockIdxController.doWork();
        // -> open
        blockIdxController.doWork();
        blockIdxController.doWork();
        blockIdxController.doWork();
        // -> snapshotting
        blockIdxController.doWork();

        assertThat(blockIdxController.isOpen()).isTrue();

        verify(mockSnapshotStorage).createSnapshot(LOG_NAME, LOG_POSITION);
        verify(mockSnapshotWriter).writeSnapshot(mockBlockIndex);
        verify(mockSnapshotWriter).commit();
    }

    @Test
    public void shouldRefuseSnapshotAndReturnToOpenStateIfFailToWriteSnapshot() throws Exception
    {
        final ByteBuffer buffer = ByteBuffer.allocate(READ_BLOCK_SIZE);
        when(mockLogStorage.read(eq(buffer), eq(LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(READ_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        when(mockLogStorage.read(any(ByteBuffer.class), eq(READ_BLOCK_SIZE + LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(INDEX_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        when(mockSnapshotPolicy.apply(anyLong())).thenReturn(true);

        doThrow(new RuntimeException("expected exception")).when(mockSnapshotWriter).writeSnapshot(mockBlockIndex);

        blockIdxController.openAsync();
        // -> opening
        blockIdxController.doWork();
        // -> open
        blockIdxController.doWork();
        blockIdxController.doWork();
        // -> create
        blockIdxController.doWork();
        // -> snapshotting
        blockIdxController.doWork();

        assertThat(blockIdxController.isOpen()).isTrue();

        verify(mockSnapshotWriter).abort();
    }

    @Test
    public void shouldNotAddBlockForLessThenHalfFullBlock() throws Exception
    {
        final ByteBuffer buffer = ByteBuffer.allocate(READ_BLOCK_SIZE);
        when(mockLogStorage.read(eq(buffer), eq(LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(READ_BLOCK_SIZE - 1 + LOG_ADDRESS, READ_BLOCK_SIZE - 1));

        blockIdxController.openAsync();
        // -> opening
        blockIdxController.doWork();
        // -> open
        blockIdxController.doWork();

        // idx for block should be created
        verify(mockBlockIndex, never()).addBlock(anyLong(), anyLong());
    }

    @Test
    public void shouldAddBlockForFullBlock() throws Exception
    {
        final ByteBuffer buffer = ByteBuffer.allocate(READ_BLOCK_SIZE);
        when(mockLogStorage.read(eq(buffer), eq(LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(READ_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        when(mockLogStorage.read(any(ByteBuffer.class), eq(READ_BLOCK_SIZE + LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(INDEX_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        blockIdxController.openAsync();
        // -> opening
        blockIdxController.doWork();
        // -> open
        blockIdxController.doWork();
        blockIdxController.doWork();
        blockIdxController.doWork();

        // idx for block should be created
        verify(mockBlockIndex).addBlock(LOG_POSITION, LOG_ADDRESS);
    }

    // TODO should not create block index for uncommitted
    // ->stay in create state until position is set

    @Test
    public void shouldNotAddBlockForFullBlockButUncommittedPosition() throws Exception
    {
        // given position is committed which is less than position of last event
        commitPosition.setOrdered(LOG_POSITION);

        final AtomicLong position = new AtomicLong(0);
        when(mockLogStorage.read(any(ByteBuffer.class), anyLong(), any(ReadResultProcessor.class)))
            .thenAnswer(readEvent(() -> position.getAndIncrement()));

        blockIdxController.openAsync();
        // -> opening
        blockIdxController.doWork();

        // when blocks are read and create state is executed
        blockIdxController.doWork();
        blockIdxController.doWork();
        blockIdxController.doWork();

        // then controller is still in create state
        assertThat(blockIdxController.isInCreateState()).isTrue();
        verify(mockBlockIndex, never()).addBlock(anyLong(), anyLong());

        // when commit position is set and create state is executed
        commitPosition.setOrdered(LOG_POSITION + 1);
        blockIdxController.doWork();

        // then
        verify(mockBlockIndex).addBlock(LOG_POSITION, LOG_ADDRESS);
    }

    @Test
    public void shouldAddHalfFullBlockForHalfDeviation() throws Exception
    {
        // given log block index controller with half deviation
        final FsLogStreamBuilder builder = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID)
            .agentRunnerService(mockAgentRunnerService)
            .logStreamControllerDisabled(true)
            .logStorage(mockLogStorage)
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .readBlockSize(READ_BLOCK_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE)
            .deviation(0.5f);

        final LogBlockIndexController blockIndexController = new LogBlockIndexController(builder, commitPosition);

        final ByteBuffer buffer = ByteBuffer.allocate(READ_BLOCK_SIZE);
        when(mockLogStorage.read(eq(buffer), eq(LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(READ_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        blockIndexController.openAsync();
        // -> opening
        blockIndexController.doWork();
        // -> open
        blockIndexController.doWork();
        blockIndexController.doWork();

        // idx for block should be created
        verify(mockBlockIndex).addBlock(LOG_POSITION, LOG_ADDRESS);
    }

    @Test
    public void shouldNotAddHalfFullBlockForQuarterDeviation() throws Exception
    {
        // given log block index controller with half deviation
        final FsLogStreamBuilder builder = new FsLogStreamBuilder(TOPIC_NAME_BUFFER, PARTITION_ID)
            .agentRunnerService(mockAgentRunnerService)
            .logStreamControllerDisabled(true)
            .logStorage(mockLogStorage)
            .snapshotStorage(mockSnapshotStorage)
            .snapshotPolicy(mockSnapshotPolicy)
            .logBlockIndex(mockBlockIndex)
            .readBlockSize(READ_BLOCK_SIZE)
            .indexBlockSize(INDEX_BLOCK_SIZE)
            .deviation(0.25f);

        final LogBlockIndexController blockIndexController = new LogBlockIndexController(builder);

        final ByteBuffer buffer = ByteBuffer.allocate(READ_BLOCK_SIZE);
        when(mockLogStorage.read(eq(buffer), eq(LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(READ_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        blockIndexController.openAsync();
        // -> opening
        blockIndexController.doWork();
        // -> open
        blockIndexController.doWork();
        blockIndexController.doWork();

        // idx for block should be created
        verify(mockBlockIndex, never()).addBlock(LOG_POSITION, LOG_ADDRESS);
    }

    @Test
    public void shouldNotAddBlockToIndexIfLimitIsNotReached() throws Exception
    {
        // given
        final ByteBuffer buffer = ByteBuffer.allocate(READ_BLOCK_SIZE);
        when(mockLogStorage.read(eq(buffer), eq(LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(READ_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        when(mockLogStorage.read(any(ByteBuffer.class), eq(READ_BLOCK_SIZE + LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(INDEX_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        when(mockLogStorage.read(any(ByteBuffer.class), eq(INDEX_BLOCK_SIZE + LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(INDEX_BLOCK_SIZE + 2 * LOG_ADDRESS, (int) LOG_ADDRESS));

        blockIdxController.openAsync();
        // opening
        blockIdxController.doWork();
        // read first block
        blockIdxController.doWork();
        blockIdxController.doWork();
        // read second small block
        blockIdxController.doWork();

        // idx for first block is created
        verify(mockBlockIndex).addBlock(LOG_POSITION, LOG_ADDRESS);
        // second idx is not created since block is not full enough
        verify(mockBlockIndex, never()).addBlock(LOG_POSITION, READ_BLOCK_SIZE + LOG_ADDRESS);
    }

    @Test
    public void shouldTruncateBeforeBlockIndexWasCreated()
    {
        when(mockLogStorage.getFirstBlockAddress()).thenReturn(LOG_ADDRESS);
        blockIdxController.openAsync();
        // -> opening
        blockIdxController.doWork();
        assertThat(blockIdxController.getNextAddress()).isGreaterThanOrEqualTo(LOG_ADDRESS);

        // when truncate is called
        final CompletableFuture<Void> truncateFuture = blockIdxController.truncate();
        blockIdxController.doWork();

        // then truncate was successful and controller is again in open state
        assertThat(truncateFuture.isDone()).isTrue();
        assertThat(blockIdxController.isOpen()).isTrue();

        // next address was reset
        assertThat(blockIdxController.getNextAddress()).isEqualTo(INVALID_ADDRESS);

        // read again from LOG_ADDRESS
        when(mockLogStorage.read(any(ByteBuffer.class), eq(LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(READ_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));
        blockIdxController.doWork();
        assertThat(blockIdxController.getNextAddress()).isGreaterThan(READ_BLOCK_SIZE);
    }

    @Test
    public void shouldTruncateAfterBlockIndexWasCreated() throws Exception
    {
        final ByteBuffer buffer = ByteBuffer.allocate(READ_BLOCK_SIZE);
        when(mockLogStorage.read(eq(buffer), eq(LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(READ_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        when(mockLogStorage.read(any(ByteBuffer.class), eq(READ_BLOCK_SIZE + LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(INDEX_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        when(mockLogStorage.read(any(ByteBuffer.class), eq(READ_BLOCK_SIZE * 2 + LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(READ_BLOCK_SIZE * 3 + LOG_ADDRESS, READ_BLOCK_SIZE));

        // given block index controller which process a block and created a block index
        // the begin address of the next block index is saved after that
        blockIdxController.openAsync();
        // -> opening
        blockIdxController.doWork();
        // -> open
        blockIdxController.doWork();
        blockIdxController.doWork();
        blockIdxController.doWork();

        // idx for block should be created
        verify(mockBlockIndex).addBlock(LOG_POSITION, LOG_ADDRESS);
        assertThat(blockIdxController.getNextAddress()).isEqualTo(READ_BLOCK_SIZE * 2 + LOG_ADDRESS);
        blockIdxController.doWork();
        assertThat(blockIdxController.getNextAddress()).isEqualTo(READ_BLOCK_SIZE * 3 + LOG_ADDRESS);

        // when truncate is called
        final CompletableFuture<Void> truncateFuture = blockIdxController.truncate();
        blockIdxController.doWork();

        // then truncate was successful and controller is again in open state
        assertThat(truncateFuture.isDone()).isTrue();
        assertThat(blockIdxController.isOpen()).isTrue();

        // next address was reset to begin of the next block
        // address of event which is not in the block for which an index was created
        assertThat(blockIdxController.getNextAddress()).isEqualTo(READ_BLOCK_SIZE * 2 + LOG_ADDRESS);

        // read again
        blockIdxController.doWork();
        assertThat(blockIdxController.getNextAddress()).isEqualTo(READ_BLOCK_SIZE * 3 + LOG_ADDRESS);
    }

    @Test
    public void shouldNotTruncateIfStateIsNeitherOpenNorCreate() throws Exception
    {
        // given not open block index controller
        assertThat(blockIdxController.isOpen()).isFalse();

        // when truncate is called
        final CompletableFuture<Void> truncateFuture = blockIdxController.truncate();
        // -> try to truncate
        blockIdxController.doWork();

        // then future is completed exceptionally and controller is not open
        assertThat(truncateFuture.isCompletedExceptionally()).isTrue();
        assertThat(blockIdxController.isOpen()).isFalse();
    }

    @Test
    public void shouldTruncateInCreateStateBeforeBlockIndexWasCreated()
    {
        commitPosition.setOrdered(INVALID_ADDRESS);
        when(mockLogStorage.read(any(ByteBuffer.class), eq(LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(READ_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        when(mockLogStorage.read(any(ByteBuffer.class), eq(READ_BLOCK_SIZE + LOG_ADDRESS), any(ReadResultProcessor.class)))
            .thenAnswer(readTwoEvents(INDEX_BLOCK_SIZE + LOG_ADDRESS, READ_BLOCK_SIZE));

        // given
        // block index controller, which process a block and waits for commit position to create a block index
        // the begin address of the next block index is saved after that
        blockIdxController.openAsync();
        blockIdxController.doWork();
        blockIdxController.doWork();
        blockIdxController.doWork();
        blockIdxController.doWork();

        // idx for block should not be created
        assertThat(blockIdxController.isInCreateState()).isTrue();
        verify(mockBlockIndex, never()).addBlock(LOG_POSITION, LOG_ADDRESS);
        assertThat(blockIdxController.getNextAddress()).isEqualTo(INDEX_BLOCK_SIZE + LOG_ADDRESS);

        // when truncate is called
        final CompletableFuture<Void> truncateFuture = blockIdxController.truncate();
        blockIdxController.doWork();

        // then truncate was successful and controller is again in open state
        assertThat(truncateFuture.isDone()).isTrue();
        assertThat(blockIdxController.isOpen()).isTrue();

        // next address was reset
        assertThat(blockIdxController.getNextAddress()).isEqualTo(LOG_ADDRESS);

        // read again
        blockIdxController.doWork();
        assertThat(blockIdxController.getNextAddress()).isEqualTo(READ_BLOCK_SIZE + LOG_ADDRESS);
        commitPosition.setOrdered(Long.MAX_VALUE);
    }

}
