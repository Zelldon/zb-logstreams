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
package io.zeebe.logstreams.processor;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import io.zeebe.logstreams.log.*;
import io.zeebe.logstreams.spi.*;
import io.zeebe.util.DeferredCommandContext;
import io.zeebe.util.actor.ActorReference;
import io.zeebe.util.actor.Actor;
import io.zeebe.util.actor.ActorScheduler;
import io.zeebe.util.state.*;

public class StreamProcessorController implements Actor
{
    protected static final int TRANSITION_DEFAULT = 0;
    protected static final int TRANSITION_OPEN = 1;
    protected static final int TRANSITION_CLOSE = 2;
    protected static final int TRANSITION_FAIL = 3;
    protected static final int TRANSITION_PROCESS = 4;
    protected static final int TRANSITION_SNAPSHOT = 5;
    protected static final int TRANSITION_RECOVER = 6;

    protected final State<Context> openingState = new OpeningState();
    protected final State<Context> openedState = new OpenedState();
    protected final State<Context> processState = new ProcessState();
    protected final State<Context> handleProcessingFailureState = new HandleStreamProcessorFailureState();
    protected final State<Context> snapshottingState = new SnapshottingState();
    protected final State<Context> recoveringState = new RecoveringState();
    protected final State<Context> reprocessingState = new ReprocessingState();
    protected final State<Context> closingSnapshottingState = new ClosingSnapshottingState();
    protected final State<Context> closingState = new ClosingState();
    protected final State<Context> closedState = new ClosedState();
    protected final State<Context> failedState = new FailedState();

    protected final StateMachineAgent<Context> stateMachineAgent = new StateMachineAgent<>(StateMachine.<Context> builder(s -> new Context(s))
            .initialState(closedState)
            .from(openingState).take(TRANSITION_DEFAULT).to(recoveringState)
            .from(openingState).take(TRANSITION_FAIL).to(failedState)
            .from(recoveringState).take(TRANSITION_DEFAULT).to(reprocessingState)
            .from(recoveringState).take(TRANSITION_FAIL).to(failedState)
            .from(reprocessingState).take(TRANSITION_DEFAULT).to(openedState)
            .from(reprocessingState).take(TRANSITION_FAIL).to(failedState)
            .from(openedState).take(TRANSITION_PROCESS).to(processState)
            .from(openedState).take(TRANSITION_CLOSE).to(closingSnapshottingState)
            .from(openedState).take(TRANSITION_FAIL).to(failedState)
            .from(processState).take(TRANSITION_DEFAULT).to(openedState)
            .from(processState).take(TRANSITION_SNAPSHOT).to(snapshottingState)
            .from(processState).take(TRANSITION_FAIL).to(handleProcessingFailureState)
            .from(processState).take(TRANSITION_CLOSE).to(closingSnapshottingState)
            .from(handleProcessingFailureState).take(TRANSITION_DEFAULT).to(openedState)
            .from(handleProcessingFailureState).take(TRANSITION_FAIL).to(failedState)
            .from(snapshottingState).take(TRANSITION_DEFAULT).to(openedState)
            .from(snapshottingState).take(TRANSITION_FAIL).to(failedState)
            .from(snapshottingState).take(TRANSITION_CLOSE).to(closingSnapshottingState)
            .from(failedState).take(TRANSITION_CLOSE).to(closedState)
            .from(failedState).take(TRANSITION_OPEN).to(openedState)
            .from(failedState).take(TRANSITION_RECOVER).to(recoveringState)
            .from(closingSnapshottingState).take(TRANSITION_DEFAULT).to(closingState)
            .from(closingSnapshottingState).take(TRANSITION_FAIL).to(closingState)
            .from(closingState).take(TRANSITION_DEFAULT).to(closedState)
            .from(closedState).take(TRANSITION_OPEN).to(openingState)
            .build());

    protected final StreamProcessor streamProcessor;
    protected final StreamProcessorContext streamProcessorContext;

    protected final DeferredCommandContext streamProcessorCmdQueue;

    protected final LogStreamReader sourceLogStreamReader;
    protected final LogStreamReader targetLogStreamReader;
    protected final LogStreamWriter logStreamWriter;

    protected final SnapshotPolicy snapshotPolicy;
    protected final SnapshotStorage snapshotStorage;
    protected final SnapshotPositionProvider snapshotPositionProvider;

    protected final LogStreamFailureListener targetLogStreamFailureListener = new TargetLogStreamFailureListener();

    protected final StreamProcessorErrorHandler streamProcessorErrorHandler;

    protected final ActorScheduler actorScheduler;
    protected ActorReference actorRef;
    protected final AtomicBoolean isRunning = new AtomicBoolean(false);

    protected final EventFilter eventFilter;
    protected final EventFilter reprocessingEventFilter;
    protected final boolean isReadOnlyProcessor;

    public StreamProcessorController(StreamProcessorContext context)
    {
        this.streamProcessorContext = context;
        this.actorScheduler = context.getTaskScheduler();
        this.streamProcessor = context.getStreamProcessor();
        this.sourceLogStreamReader = context.getSourceLogStreamReader();
        this.targetLogStreamReader = context.getTargetLogStreamReader();
        this.logStreamWriter = context.getLogStreamWriter();
        this.snapshotPolicy = context.getSnapshotPolicy();
        this.snapshotStorage = context.getSnapshotStorage();
        this.snapshotPositionProvider = context.getSnapshotPositionProvider();
        this.streamProcessorCmdQueue = context.getStreamProcessorCmdQueue();
        this.eventFilter = context.getEventFilter();
        this.reprocessingEventFilter = context.getReprocessingEventFilter();
        this.isReadOnlyProcessor = context.isReadOnlyProcessor();
        this.streamProcessorErrorHandler = context.getErrorHandler();
    }

    @Override
    public int doWork()
    {
        return stateMachineAgent.doWork();
    }

    @Override
    public String name()
    {
        return streamProcessorContext.getName();
    }

    public CompletableFuture<Void> openAsync()
    {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        stateMachineAgent.addCommand(context ->
        {
            final boolean opening = context.tryTake(TRANSITION_OPEN);
            if (opening)
            {
                context.setFuture(future);
            }
            else
            {
                future.completeExceptionally(new IllegalStateException("Cannot open stream processor."));
            }
        });

        if (isRunning.compareAndSet(false, true))
        {
            try
            {
                actorRef = actorScheduler.schedule(this);
            }
            catch (Exception e)
            {
                isRunning.set(false);
                future.completeExceptionally(e);
            }
        }
        return future;
    }

    public CompletableFuture<Void> closeAsync()
    {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        stateMachineAgent.addCommand(context ->
        {
            final boolean closing = context.tryTake(TRANSITION_CLOSE);
            if (closing)
            {
                context.setFuture(future);
            }
            else
            {
                future.completeExceptionally(new IllegalStateException("Cannot close stream processor."));
            }
        });

        return future;
    }

    public boolean isOpen()
    {
        return stateMachineAgent.getCurrentState() == openedState
                || stateMachineAgent.getCurrentState() == processState
                || stateMachineAgent.getCurrentState() == snapshottingState;
    }

    public boolean isClosing()
    {
        return stateMachineAgent.getCurrentState() == closingState
                || stateMachineAgent.getCurrentState() == closingSnapshottingState;
    }

    public boolean isClosed()
    {
        return stateMachineAgent.getCurrentState() == closedState;
    }

    public boolean isFailed()
    {
        return stateMachineAgent.getCurrentState() == failedState;
    }

    protected boolean isSourceStreamWriter()
    {
        final LogStream sourceStream = streamProcessorContext.getSourceStream();
        final LogStream targetStream = streamProcessorContext.getTargetStream();

        return targetStream.getPartitionId() == sourceStream.getPartitionId() && targetStream.getTopicName().equals(sourceStream.getTopicName());
    }

    public EventFilter getEventFilter()
    {
        return eventFilter;
    }

    public EventFilter getReprocessingEventFilter()
    {
        return reprocessingEventFilter;
    }

    protected final BiConsumer<Context, Exception> stateFailureHandler = (context, e) ->
    {
        e.printStackTrace();

        context.take(TRANSITION_FAIL);
        context.completeFutureExceptionally(e);
    };

    private class OpeningState implements TransitionState<Context>
    {
        @Override
        public void work(Context context)
        {
            final LogStream targetStream = streamProcessorContext.getTargetStream();

            targetLogStreamReader.wrap(targetStream);
            logStreamWriter.wrap(targetStream);
            sourceLogStreamReader.wrap(streamProcessorContext.getSourceStream());

            targetStream.removeFailureListener(targetLogStreamFailureListener);
            targetStream.registerFailureListener(targetLogStreamFailureListener);

            context.take(TRANSITION_DEFAULT);
        }

        @Override
        public void onFailure(Context context, Exception e)
        {
            stateFailureHandler.accept(context, e);
        }
    }

    private class OpenedState implements State<Context>
    {
        @Override
        public int doWork(Context context)
        {
            int workCount = 0;

            workCount += streamProcessorCmdQueue.doWork();

            if (!streamProcessor.isSuspended() && sourceLogStreamReader.hasNext())
            {
                workCount += 1;

                final LoggedEvent event = sourceLogStreamReader.next();
                context.setEvent(event);

                if (eventFilter == null || eventFilter.applies(event))
                {
                    context.take(TRANSITION_PROCESS);
                }
            }

            return workCount;
        }

        @Override
        public void onFailure(Context context, Exception e)
        {
            stateFailureHandler.accept(context, e);
        }
    }

    private class ProcessState extends ComposedState<Context>
    {
        private EventProcessor eventProcessor;
        private long eventPosition;

        @Override
        protected List<Step<Context>> steps()
        {
            return Arrays.asList(
                    processEventStep,
                    sideEffectsStep,
                    writeEventStep,
                    updateStateStep);
        }

        private Step<Context> processEventStep = context ->
        {
            boolean processEvent = false;

            eventProcessor = streamProcessor.onEvent(context.getEvent());

            if (eventProcessor != null)
            {
                eventProcessor.processEvent();
                processEvent = true;
            }
            else
            {
                context.take(TRANSITION_DEFAULT);
            }
            return processEvent;
        };

        private Step<Context> sideEffectsStep = context -> eventProcessor.executeSideEffects();

        private Step<Context> writeEventStep = context ->
        {
            final LogStream sourceStream = streamProcessorContext.getSourceStream();
            logStreamWriter
                .producerId(streamProcessorContext.getId())
                .sourceEvent(sourceStream.getTopicName(), sourceStream.getPartitionId(), context.getEvent().getPosition());

            eventPosition = eventProcessor.writeEvent(logStreamWriter);
            return eventPosition >= 0;
        };

        private FailSafeStep<Context> updateStateStep = context ->
        {
            eventProcessor.updateState();

            streamProcessor.afterEvent();

            final boolean hasWrittenEvent = eventPosition > 0;
            if (hasWrittenEvent)
            {
                context.setLastWrittenEventPosition(eventPosition);
            }

            if (hasWrittenEvent && snapshotPolicy.apply(context.getEvent().getPosition()))
            {
                context.take(TRANSITION_SNAPSHOT);
            }
            else
            {
                context.take(TRANSITION_DEFAULT);
            }
        };

        @Override
        public void onFailure(Context context, Exception e)
        {
            context.setFailure(e);
            context.take(TRANSITION_FAIL);
        }
    }

    private class HandleStreamProcessorFailureState implements TransitionState<Context>
    {
        @Override
        public void work(Context context) throws Exception
        {
            final int result = streamProcessorErrorHandler.onError(context.getEvent(), context.getFailure());

            if (result == StreamProcessorErrorHandler.RESULT_SUCCESS)
            {
                context.take(TRANSITION_DEFAULT);
            }
            else if (result == StreamProcessorErrorHandler.RESULT_FAILURE)
            {
                // retry
            }
            else
            {
                System.err.println(String.format("The log stream processor '%s' failed to process event. It stop processing further events.", streamProcessorContext.getName()));
                context.getFailure().printStackTrace();

                context.take(TRANSITION_FAIL);
            }
        }

        @Override
        public void onFailure(Context context, Exception e)
        {
            stateFailureHandler.accept(context, e);
        }
    }

    /**
     * @param context
     * @return true if successful
     */
    protected boolean ensureSnapshotWritten(Context context)
    {
        boolean isSnapshotWritten = false;

        final long lastWrittenEventPosition = context.getLastWrittenEventPosition();
        final long commitPosition = streamProcessorContext.getTargetStream().getCommitPosition();

        final long snapshotPosition = snapshotPositionProvider.getSnapshotPosition(context.getEvent(), lastWrittenEventPosition);
        final boolean snapshotAlreadyPresent = snapshotPosition <= context.getSnapshotPosition();

        if (!snapshotAlreadyPresent)
        {
            if (commitPosition >= lastWrittenEventPosition)
            {
                writeSnapshot(context, snapshotPosition);

                isSnapshotWritten = true;
            }
        }
        else
        {
            isSnapshotWritten = true;
        }

        return isSnapshotWritten;
    }

    protected void writeSnapshot(final Context context, final long eventPosition)
    {
        SnapshotWriter snapshotWriter = null;
        try
        {
            snapshotWriter = snapshotStorage.createSnapshot(streamProcessorContext.getName(), eventPosition);

            snapshotWriter.writeSnapshot(streamProcessor.getStateResource());
            snapshotWriter.commit();
            context.setSnapshotPosition(eventPosition);
        }
        catch (Exception e)
        {
            e.printStackTrace();

            if (snapshotWriter != null)
            {
                snapshotWriter.abort();
            }
        }
    }

    private class SnapshottingState implements State<Context>
    {
        @Override
        public int doWork(Context context)
        {
            int workCount = 0;

            final boolean snapshotWritten = ensureSnapshotWritten(context);

            if (snapshotWritten)
            {
                context.take(TRANSITION_DEFAULT);
                workCount += 1;
            }

            return workCount;
        }
    }

    private class RecoveringState implements TransitionState<Context>
    {
        @Override
        public void work(Context context) throws Exception
        {
            streamProcessor.getStateResource().reset();

            long snapshotPosition = -1;

            final ReadableSnapshot lastSnapshot = snapshotStorage.getLastSnapshot(streamProcessorContext.getName());

            if (lastSnapshot != null)
            {
                // recover last snapshot
                lastSnapshot.recoverFromSnapshot(streamProcessor.getStateResource());

                // read the last event from snapshot
                snapshotPosition = lastSnapshot.getPosition();
                final boolean found = targetLogStreamReader.seek(snapshotPosition);

                if (found && targetLogStreamReader.hasNext())
                {
                    final LoggedEvent lastEventFromSnapshot = targetLogStreamReader.next();

                    // resume the next position on source log stream to continue from
                    final long sourceEventPosition = isSourceStreamWriter() ? snapshotPosition : lastEventFromSnapshot.getSourceEventPosition();
                    sourceLogStreamReader.seek(sourceEventPosition + 1);
                }
                else
                {
                    throw new IllegalStateException(
                            String.format("Stream processor '%s' failed to recover. Cannot find event with the snapshot position in target log stream.",
                                          streamProcessorContext.getName()));
                }
            }
            context.setSnapshotPosition(snapshotPosition);

            context.take(TRANSITION_DEFAULT);
        }

        @Override
        public void onFailure(Context context, Exception e)
        {
            stateFailureHandler.accept(context, e);
        }
    }

    private class ReprocessingState implements State<Context>
    {
        @Override
        public int doWork(Context context)
        {
            // for read-only processors, we don't need to scan the log for further events
            // that they might have written
            if (!isReadOnlyProcessor && targetLogStreamReader.hasNext())
            {
                final LoggedEvent targetEvent = targetLogStreamReader.next();
                processEvent(context, targetEvent);
            }
            else
            {
                // all events are re-processed
                streamProcessor.onOpen(streamProcessorContext);

                context.take(TRANSITION_DEFAULT);
                context.completeFuture();
            }
            return 1;
        }

        protected void processEvent(Context context, final LoggedEvent targetEvent)
        {
            // ignore events from other producers
            if (targetEvent.getProducerId() == streamProcessorContext.getId() &&
                    (reprocessingEventFilter == null || reprocessingEventFilter.applies(targetEvent)))
            {
                final long sourceEventPosition = targetEvent.getSourceEventPosition();

                if (isSourceStreamWriter() && sourceEventPosition <= context.getSnapshotPosition())
                {
                    // ignore the event when it was processed before creating the snapshot
                    return;
                }

                // seek to the source event (assuming that the reader is near the position)
                LoggedEvent sourceEvent = null;
                long currentSourceEventPosition = -1;
                while (sourceLogStreamReader.hasNext() && currentSourceEventPosition < sourceEventPosition)
                {
                    sourceEvent = sourceLogStreamReader.next();
                    currentSourceEventPosition = sourceEvent.getPosition();
                }

                if (sourceEvent != null && currentSourceEventPosition == sourceEventPosition)
                {
                    // re-process the event from source stream
                    final EventProcessor eventProcessor = streamProcessor.onEvent(sourceEvent);
                    eventProcessor.processEvent();
                    eventProcessor.updateState();
                    streamProcessor.afterEvent();
                }
                else
                {
                    // source or target log is maybe corrupted
                    throw new IllegalStateException(String.format("Stream processor '%s' failed to reprocess. Cannot find source event of written event: %s",
                                                                  streamProcessorContext.getName(), targetEvent));
                }
            }
        }

        @Override
        public void onFailure(Context context, Exception e)
        {
            stateFailureHandler.accept(context, e);
        }
    }

    private class ClosingSnapshottingState implements State<Context>
    {
        @Override
        public int doWork(Context context) throws Exception
        {
            int workCount = 0;

            final boolean hasProcessedAnyEvent = context.getEvent() != null;

            if (hasProcessedAnyEvent)
            {
                ensureSnapshotWritten(context);
            }

            context.take(TRANSITION_DEFAULT);
            workCount += 1;

            return workCount;
        }
    }

    private class ClosingState implements TransitionState<Context>
    {
        @Override
        public void work(Context context)
        {
            streamProcessor.onClose();

            streamProcessorContext.getTargetStream().removeFailureListener(targetLogStreamFailureListener);

            context.take(TRANSITION_DEFAULT);
        }
    }

    private class ClosedState implements WaitState<Context>
    {
        @Override
        public void work(Context context)
        {
            if (isRunning.compareAndSet(true, false))
            {
                context.completeFuture();

                actorRef.close();
            }
        }
    }

    private class FailedState implements WaitState<Context>
    {
        @Override
        public void work(Context context)
        {
            // wait for recovery
        }
    }

    private class TargetLogStreamFailureListener implements LogStreamFailureListener
    {
        @Override
        public void onFailed(long failedPosition)
        {
            stateMachineAgent.addCommand(context ->
            {
                final boolean failed = context.tryTake(TRANSITION_FAIL);
                if (failed)
                {
                    context.setFailure(new RuntimeException("log stream failure"));
                    context.setFailedEventPosition(failedPosition);
                }
            });
        }

        @Override
        public void onRecovered()
        {
            stateMachineAgent.addCommand(context ->
            {
                final long failedEventPosition = context.getFailedEventPosition();
                if (failedEventPosition < 0)
                {
                    // ignore
                }
                else if (failedEventPosition <= context.getLastWrittenEventPosition())
                {
                    context.take(TRANSITION_RECOVER);
                }
                else
                {
                    // no recovery required if the log stream failed after all events of the processor are written
                    context.take(TRANSITION_OPEN);
                }
                context.setFailedEventPosition(-1);
            });
        }
    }

    private class Context extends SimpleStateMachineContext
    {
        private LoggedEvent event;
        private long lastWrittenEventPosition = -1;
        private long snapshotPosition = -1;
        private long failedEventPosition = -1;
        private CompletableFuture<Void> future;
        private Exception failure;

        Context(StateMachine<Context> stateMachine)
        {
            super(stateMachine);
        }

        public LoggedEvent getEvent()
        {
            return event;
        }

        public void setEvent(LoggedEvent event)
        {
            this.event = event;
        }

        public void completeFuture()
        {
            if (future != null)
            {
                future.complete(null);
                future = null;
            }
        }

        public void completeFutureExceptionally(Throwable e)
        {
            if (future != null)
            {
                future.completeExceptionally(e);
                future = null;
            }
        }

        public void setFuture(CompletableFuture<Void> future)
        {
            this.future = future;
        }

        public long getLastWrittenEventPosition()
        {
            return lastWrittenEventPosition;
        }

        public void setLastWrittenEventPosition(long lastWrittenEventPosition)
        {
            this.lastWrittenEventPosition = lastWrittenEventPosition;
        }

        public long getFailedEventPosition()
        {
            return failedEventPosition;
        }

        public void setFailedEventPosition(long failedEventPosition)
        {
            this.failedEventPosition = failedEventPosition;
        }

        public void setSnapshotPosition(long snapshotPosition)
        {
            this.snapshotPosition = snapshotPosition;
        }

        public long getSnapshotPosition()
        {
            return snapshotPosition;
        }

        public Exception getFailure()
        {
            return failure;
        }

        public void setFailure(Exception failure)
        {
            this.failure = failure;
        }
    }

}