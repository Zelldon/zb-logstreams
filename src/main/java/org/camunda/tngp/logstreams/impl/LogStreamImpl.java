package org.camunda.tngp.logstreams.impl;

import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicLongPosition;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.Position;
import org.camunda.tngp.dispatcher.Dispatcher;
import org.camunda.tngp.dispatcher.Dispatchers;
import org.camunda.tngp.dispatcher.impl.PositionUtil;
import org.camunda.tngp.logstreams.impl.log.index.LogBlockIndex;
import org.camunda.tngp.logstreams.log.BufferedLogStreamReader;
import org.camunda.tngp.logstreams.log.LogStream;
import org.camunda.tngp.logstreams.log.LogStreamFailureListener;
import org.camunda.tngp.logstreams.log.LoggedEvent;
import org.camunda.tngp.logstreams.snapshot.TimeBasedSnapshotPolicy;
import org.camunda.tngp.logstreams.spi.LogStorage;
import org.camunda.tngp.logstreams.spi.SnapshotPolicy;
import org.camunda.tngp.logstreams.spi.SnapshotStorage;
import org.camunda.tngp.util.agent.AgentRunnerService;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

/**
 * Represents the implementation of the LogStream interface.
 */
public final class LogStreamImpl implements LogStream
{
    public static final String EXCEPTION_MSG_TRUNCATE_AND_LOG_STREAM_CTRL_IN_PARALLEL = "Can't truncate the log storage and have a log stream controller active at the same time.";

    protected volatile int term = 0;
    protected Position commitPosition;

    protected final LogControllerContext controllerContext;
    protected final int logId;


    protected final LogBlockIndexController logBlockIndexController;

    protected LogStreamController logStreamController;
    protected AgentRunnerService writeAgentRunnerService;
    protected Dispatcher writeBuffer;


    private LogStreamImpl(LogStreamBuilder logStreamBuilder)
    {
        this.controllerContext = logStreamBuilder.getLogControllerContext();
        this.logId = logStreamBuilder.getLogId();
        this.logBlockIndexController = new LogBlockIndexController(logStreamBuilder);

        this.commitPosition = new AtomicLongPosition();
        this.commitPosition.setOrdered(-1L);

        if (!logStreamBuilder.isLogStreamControllerDisabled())
        {
            this.logStreamController = new LogStreamController(logStreamBuilder);
            writeAgentRunnerService = logStreamBuilder.getWriteBufferAgentRunnerService();
            this.writeBuffer = logStreamBuilder.getWriteBuffer();
        }
    }


    public LogBlockIndexController getLogBlockIndexController()
    {
        return logBlockIndexController;
    }

    public LogStreamController getLogStreamController()
    {
        return logStreamController;
    }

    @Override
    public int getId()
    {
        return logId;
    }

    @Override
    public String getLogName()
    {
        return controllerContext.getName();
    }

    @Override
    public void open()
    {
        try
        {
            openAsync().get();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause() != null ? e.getCause() : e);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> openAsync()
    {
        final CompletableFuture<Void> logBlockIndexControllerFuture = logBlockIndexController.openAsync();
        final CompletableFuture<Void> completableFuture;
        if (logStreamController != null)
        {
            completableFuture = CompletableFuture.allOf(logBlockIndexControllerFuture,
                logStreamController.openAsync());
        }
        else
        {
            completableFuture = logBlockIndexControllerFuture;
        }

        return completableFuture;
    }

    @Override
    public void close()
    {
        try
        {
            closeAsync().get();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause() != null ? e.getCause() : e);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync()
    {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final BiConsumer<Void, Throwable> closeLogStorage = (n, e) ->
        {
            controllerContext.logStorageClose();
            future.complete(null);
        };

        //write buffer close
        final CompletableFuture<Void> logBlockIndexControllerClosingFuture =
            logBlockIndexController.closeAsync();
        if (logStreamController != null)
        {
            // can't close dispatcher since has no method to reopen
            // TODO camunda-tngp/dispatcher#12
            CompletableFuture.allOf(logBlockIndexControllerClosingFuture,
                logStreamController.closeAsync())
                .whenComplete(closeLogStorage);
        }
        else
        {
            logBlockIndexControllerClosingFuture.whenComplete(closeLogStorage);
        }

        return future;
    }

    @Override
    public long getCurrentAppenderPosition()
    {
        return logStreamController == null ? 0 : logStreamController.getCurrentAppenderPosition();
    }

    @Override
    public long getCommitPosition()
    {
        return commitPosition.get();
    }

    @Override
    public void setCommitPosition(long commitPosition)
    {
        this.commitPosition.setOrdered(commitPosition);
    }

    @Override
    public int getTerm()
    {
        return term;
    }

    @Override
    public void setTerm(int term)
    {
        this.term = term;
    }

    @Override
    public void registerFailureListener(LogStreamFailureListener listener)
    {
        if (logStreamController != null)
        {
            logStreamController.registerFailureListener(listener);
        }
    }

    @Override
    public void removeFailureListener(LogStreamFailureListener listener)
    {
        if (logStreamController != null)
        {
            logStreamController.removeFailureListener(listener);
        }
    }

    @Override
    public LogStorage getLogStorage()
    {
        return controllerContext.getLogStorage();
    }

    @Override
    public LogBlockIndex getLogBlockIndex()
    {
        return controllerContext.getBlockIndex();
    }

    @Override
    public int getIndexBlockSize()
    {
        return logBlockIndexController.getIndexBlockSize();
    }

    @Override
    public CompletableFuture<Void> closeLogStreamController()
    {
        final CompletableFuture<Void> completableFuture;
        if (logStreamController != null)
        {
            completableFuture = new CompletableFuture<>();
//            Dispatcher should be closed, but this will fail the integration tests
//            see TODO camunda-tngp/logstreams#60
//            writeBuffer.closeAsync()
//                       .thenCompose((v) ->
            logStreamController.closeAsync()
                .handle((v, ex) ->
                {
                    writeBuffer = null;
                    logStreamController = null;
                    return ex != null
                        ? completableFuture.completeExceptionally(ex)
                        : completableFuture.complete(null);
                });
        }
        else
        {
            completableFuture = CompletableFuture.completedFuture(null);
        }
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> openLogStreamController()
    {

        return openLogStreamController(writeAgentRunnerService, DEFAULT_MAX_APPEND_BLOCK_SIZE);
    }

    @Override
    public CompletableFuture<Void> openLogStreamController(AgentRunnerService writeBufferAgentRunnerService)
    {
        return openLogStreamController(writeBufferAgentRunnerService, DEFAULT_MAX_APPEND_BLOCK_SIZE);
    }

    @Override
    public CompletableFuture<Void> openLogStreamController(AgentRunnerService writeBufferAgentRunnerService,
                                                           int maxAppendBlockSize)
    {
        final LogStreamBuilder logStreamBuilder = new LogStreamBuilder(controllerContext.getName(), logId)
            .logControllerContext(controllerContext)
            .writeBufferAgentRunnerService(writeBufferAgentRunnerService)
            .maxAppendBlockSize(maxAppendBlockSize);

        this.logStreamController = new LogStreamController(logStreamBuilder);
        this.writeBuffer = logStreamBuilder.getWriteBuffer();
        return logStreamController.openAsync();
    }

    @Override
    public Dispatcher getWriteBuffer()
    {
        return logStreamController == null ? null : logStreamController.getWriteBuffer();
    }

    @Override
    public CompletableFuture<Void> truncate(long position)
    {
        if (logStreamController != null)
        {
            throw new IllegalStateException(EXCEPTION_MSG_TRUNCATE_AND_LOG_STREAM_CTRL_IN_PARALLEL);
        }
        return logBlockIndexController.truncate(position);
    }

    // BUILDER ////////////////////////
    public static class LogStreamBuilder<T extends LogStreamBuilder<T>>
    {
        // MANDATORY /////
        // LogController Base
        protected final String logName;
        protected final int logId;
        protected AgentRunnerService agentRunnerService;
        protected LogStorage logStorage;
        protected LogBlockIndex logBlockIndex;

        protected LogControllerContext logControllerContext;

        protected String logRootPath;
        protected String logDirectory;

        protected CountersManager countersManager;

        // OPTIONAL ////////////////////////////////////////////
        protected boolean logStreamControllerDisabled;
        protected int initialLogSegmentId = 0;
        protected boolean deleteOnClose;
        protected int maxAppendBlockSize = 1024 * 1024 * 4;
        protected int writeBufferSize = 1024 * 1024 * 16;
        protected int logSegmentSize = 1024 * 1024 * 128;
        protected int indexBlockSize = 1024 * 32;
        protected int readBlockSize = 1024 * 32;
        protected SnapshotPolicy snapshotPolicy;
        protected SnapshotStorage snapshotStorage;

        protected AgentRunnerService writeBufferAgentRunnerService;
        protected Dispatcher writeBuffer;

        public LogStreamBuilder(String logName, int logId)
        {
            this.logName = logName;
            this.logId = logId;
        }

        public T logRootPath(String logRootPath)
        {
            this.logRootPath = logRootPath;
            return (T) this;
        }

        public T logDirectory(String logDir)
        {
            this.logDirectory = logDir;
            return (T) this;
        }

        public T writeBufferSize(int writeBufferSize)
        {
            this.writeBufferSize = writeBufferSize;
            return (T) this;
        }

        public T maxAppendBlockSize(int maxAppendBlockSize)
        {
            this.maxAppendBlockSize = maxAppendBlockSize;
            return (T) this;
        }

        public T writeBufferAgentRunnerService(AgentRunnerService writeBufferAgentRunnerService)
        {
            this.writeBufferAgentRunnerService = writeBufferAgentRunnerService;
            return (T) this;
        }

        public T initialLogSegmentId(int logFragmentId)
        {
            this.initialLogSegmentId = logFragmentId;
            return (T) this;
        }

        public T logSegmentSize(int logSegmentSize)
        {
            this.logSegmentSize = logSegmentSize;
            return (T) this;
        }

        public T deleteOnClose(boolean deleteOnClose)
        {
            this.deleteOnClose = deleteOnClose;
            return (T) this;
        }

        public T agentRunnerService(AgentRunnerService agentRunnerService)
        {
            this.agentRunnerService = agentRunnerService;
            return (T) this;
        }

        public T countersManager(CountersManager countersManager)
        {
            this.countersManager = countersManager;
            return (T) this;
        }

        public T indexBlockSize(int indexBlockSize)
        {
            this.indexBlockSize = indexBlockSize;
            return (T) this;
        }

        public T logStorage(LogStorage logStorage)
        {
            this.logStorage = logStorage;
            return (T) this;
        }

        public T logBlockIndex(LogBlockIndex logBlockIndex)
        {
            this.logBlockIndex = logBlockIndex;
            return (T) this;
        }

        public T logControllerContext(LogControllerContext logControllerContext)
        {
            this.logControllerContext = logControllerContext;
            return (T) this;
        }

        public T logStreamControllerDisabled(boolean logStreamControllerDisabled)
        {
            this.logStreamControllerDisabled = logStreamControllerDisabled;
            return (T) this;
        }

        public T writeBuffer(Dispatcher writeBuffer)
        {
            this.writeBuffer = writeBuffer;
            return (T) this;
        }

        public T snapshotStorage(SnapshotStorage snapshotStorage)
        {
            this.snapshotStorage = snapshotStorage;
            return (T) this;
        }

        public T snapshotPolicy(SnapshotPolicy snapshotPolicy)
        {
            this.snapshotPolicy = snapshotPolicy;
            return (T) this;
        }

        public T readBlockSize(int readBlockSize)
        {
            this.readBlockSize = readBlockSize;
            return (T) this;
        }

        // getter /////////////////

        public String getLogName()
        {
            return logName;
        }

        public int getLogId()
        {
            return logId;
        }

        public AgentRunnerService getAgentRunnerService()
        {
            if (agentRunnerService == null)
            {
                if (logControllerContext == null)
                {
                    Objects.requireNonNull(agentRunnerService, "No agent runner service provided.");
                }
                else
                {
                    agentRunnerService = logControllerContext.getAgentRunnerService();
                }
            }

            return agentRunnerService;
        }

        protected void initLogStorage()
        {
        }

        public LogStorage getLogStorage()
        {
            if (logStorage == null)
            {
                if (logControllerContext == null)
                {
                    initLogStorage();
                }
                else
                {
                    logStorage = logControllerContext.getLogStorage();
                }
            }
            return logStorage;
        }

        public LogBlockIndex getBlockIndex()
        {
            if (logBlockIndex == null)
            {
                if (logControllerContext == null)
                {
                    this.logBlockIndex = new LogBlockIndex(100000, (c) -> new UnsafeBuffer(ByteBuffer.allocate(c)));
                }
                else
                {
                    this.logBlockIndex = logControllerContext.getBlockIndex();
                }
            }
            return logBlockIndex;
        }

        public LogControllerContext getLogControllerContext()
        {
            if (logControllerContext == null)
            {
                logControllerContext = new LogControllerContext(
                    getLogName(),
                    getLogStorage(),
                    getBlockIndex(),
                    getAgentRunnerService());
            }
            return logControllerContext;
        }

        public int getMaxAppendBlockSize()
        {
            return maxAppendBlockSize;
        }

        public int getIndexBlockSize()
        {
            return indexBlockSize;
        }

        public int getReadBlockSize()
        {
            return readBlockSize;
        }

        public SnapshotPolicy getSnapshotPolicy()
        {
            if (snapshotPolicy == null)
            {
                snapshotPolicy = new TimeBasedSnapshotPolicy(Duration.ofMinutes(1));
            }
            return snapshotPolicy;
        }

        public AgentRunnerService getWriteBufferAgentRunnerService()
        {
            return writeBufferAgentRunnerService;
        }

        protected Dispatcher initWriteBuffer(Dispatcher writeBuffer, BufferedLogStreamReader logReader,
                                             String logName, int writeBufferSize)
        {
            if (writeBuffer == null)
            {
                // Get position of last entry
                long lastPosition = 0;

                logReader.seekToLastEvent();

                if (logReader.hasNext())
                {
                    final LoggedEvent lastEntry = logReader.next();
                    lastPosition = lastEntry.getPosition();
                }

                // dispatcher needs to generate positions greater than the last position
                int partitionId = 0;

                if (lastPosition > 0)
                {
                    partitionId = PositionUtil.partitionId(lastPosition);
                }

                writeBuffer = Dispatchers.create("log-write-buffer-" + logName)
                    .bufferSize(writeBufferSize)
                    .subscriptions("log-appender")
                    .initialPartitionId(partitionId + 1)
                    .conductorExternallyManaged()
                    .build();
            }
            return writeBuffer;
        }

        public Dispatcher getWriteBuffer()
        {
            if (writeBuffer == null)
            {
                final BufferedLogStreamReader logReader = new BufferedLogStreamReader(getLogStorage(), getBlockIndex());
                writeBuffer = initWriteBuffer(writeBuffer, logReader, logName, writeBufferSize);
            }
            return writeBuffer;
        }

        public boolean isLogStreamControllerDisabled()
        {
            return logStreamControllerDisabled;
        }

        public void initSnapshotStorage()
        {
        }

        public SnapshotStorage getSnapshotStorage()
        {
            if (snapshotStorage == null)
            {
                initSnapshotStorage();
            }
            return snapshotStorage;
        }

        public LogStream build()
        {
            return new LogStreamImpl(this);
        }
    }
}
