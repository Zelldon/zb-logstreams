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
package org.camunda.tngp.logstreams.processor;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.concurrent.Agent;
import org.camunda.tngp.logstreams.log.LogStream;
import org.camunda.tngp.logstreams.log.LogStreamFailureListener;
import org.camunda.tngp.logstreams.log.LogStreamReader;
import org.camunda.tngp.logstreams.log.LogStreamWriter;
import org.camunda.tngp.logstreams.log.LoggedEvent;
import org.camunda.tngp.logstreams.spi.ReadableSnapshot;
import org.camunda.tngp.logstreams.spi.SnapshotPolicy;
import org.camunda.tngp.logstreams.spi.SnapshotStorage;
import org.camunda.tngp.logstreams.spi.SnapshotWriter;
import org.camunda.tngp.util.agent.AgentRunnerService;
import org.camunda.tngp.util.state.ComposedState;
import org.camunda.tngp.util.state.SimpleStateMachineContext;
import org.camunda.tngp.util.state.State;
import org.camunda.tngp.util.state.StateMachine;
import org.camunda.tngp.util.state.StateMachineAgent;
import org.camunda.tngp.util.state.TransitionState;
import org.camunda.tngp.util.state.WaitState;

public class StreamProcessorController implements Agent
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
    protected final State<Context> snapshottingState = new SnapshottingState();
    protected final State<Context> recoveringState = new RecoveringState();
    protected final State<Context> reprocessingState = new ReprocessingState();
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
            .from(openedState).take(TRANSITION_CLOSE).to(closingState)
            .from(openedState).take(TRANSITION_FAIL).to(failedState)
            .from(processState).take(TRANSITION_DEFAULT).to(openedState)
            .from(processState).take(TRANSITION_SNAPSHOT).to(snapshottingState)
            .from(processState).take(TRANSITION_FAIL).to(failedState)
            .from(processState).take(TRANSITION_CLOSE).to(closingState)
            .from(snapshottingState).take(TRANSITION_DEFAULT).to(openedState)
            .from(snapshottingState).take(TRANSITION_FAIL).to(failedState)
            .from(snapshottingState).take(TRANSITION_CLOSE).to(closingState)
            .from(failedState).take(TRANSITION_CLOSE).to(closedState)
            .from(failedState).take(TRANSITION_OPEN).to(openedState)
            .from(failedState).take(TRANSITION_RECOVER).to(recoveringState)
            .from(closingState).take(TRANSITION_DEFAULT).to(closedState)
            .from(closedState).take(TRANSITION_OPEN).to(openingState)
            .build());

    protected final StreamProcessor streamProcessor;
    protected final StreamProcessorContext streamProcessorContext;

    protected final LogStreamReader sourceLogStreamReader;
    protected final LogStreamReader targetLogStreamReader;
    protected final LogStreamWriter logStreamWriter;

    protected final SnapshotPolicy snapshotPolicy;
    protected final SnapshotStorage snapshotStorage;

    protected final LogStreamFailureListener targetLogStreamFailureListener = new TargetLogStreamFailureListener();

    protected final AgentRunnerService agentRunnerService;
    protected final AtomicBoolean isRunning = new AtomicBoolean(false);

    public StreamProcessorController(StreamProcessorContext context)
    {
        this.streamProcessorContext = context;
        this.agentRunnerService = context.getAgentRunnerService();
        this.streamProcessor = context.getStreamProcessor();
        this.sourceLogStreamReader = context.getSourceLogStreamReader();
        this.targetLogStreamReader = context.getTargetLogStreamReader();
        this.logStreamWriter = context.getLogStreamWriter();
        this.snapshotPolicy = context.getSnapshotPolicy();
        this.snapshotStorage = context.getSnapshotStorage();
    }

    @Override
    public int doWork()
    {
        return stateMachineAgent.doWork();
    }

    @Override
    public String roleName()
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
                agentRunnerService.run(this);
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

    public boolean isClosed()
    {
        return stateMachineAgent.getCurrentState() == closedState;
    }

    public boolean isFailed()
    {
        return stateMachineAgent.getCurrentState() == failedState;
    }

    private class OpeningState implements TransitionState<Context>
    {
        @Override
        public void work(Context context)
        {
            final LogStream targetStream = streamProcessorContext.getTargetStream();

            try
            {
                targetLogStreamReader.wrap(targetStream);
                logStreamWriter.wrap(targetStream);
                sourceLogStreamReader.wrap(streamProcessorContext.getSourceStream());

                targetStream.removeFailureListener(targetLogStreamFailureListener);
                targetStream.registerFailureListener(targetLogStreamFailureListener);

                context.take(TRANSITION_DEFAULT);
            }
            catch (Exception e)
            {
                e.printStackTrace();

                context.take(TRANSITION_FAIL);
                context.completeFutureExceptionally(e);
            }
        }
    }

    private class OpenedState implements State<Context>
    {
        @Override
        public int doWork(Context context)
        {
            int workCount = 0;

            if (sourceLogStreamReader.hasNext())
            {
                workCount = 1;

                final LoggedEvent event = sourceLogStreamReader.next();
                context.setEvent(event);

                context.take(TRANSITION_PROCESS);
            }

            return workCount;
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

        private FailSafeStep<Context> processEventStep = context ->
        {
            eventProcessor = streamProcessor.onEvent(context.getEvent());
            eventProcessor.processEvent();
        };

        private Step<Context> sideEffectsStep = context -> eventProcessor.executeSideEffects();

        private Step<Context> writeEventStep = context ->
        {
            logStreamWriter
                .producerId(streamProcessorContext.getId())
                .sourceEvent(streamProcessorContext.getSourceStream().getId(), context.getEvent().getPosition());

            eventPosition = eventProcessor.writeEvent(logStreamWriter);
            return eventPosition >= 0;
        };

        private FailSafeStep<Context> updateStateStep = context ->
        {
            eventProcessor.updateState();

            streamProcessor.afterEvent();

            context.setLastWrittenEventPosition(eventPosition);

            if (snapshotPolicy.apply(context.getEvent().getPosition()))
            {
                context.take(TRANSITION_SNAPSHOT);
            }
            else
            {
                context.take(TRANSITION_DEFAULT);
            }
        };
    }

    private class SnapshottingState implements State<Context>
    {
        @Override
        public int doWork(Context context)
        {
            int workCount = 0;

            final long lastWrittenEventPosition = context.getLastWrittenEventPosition();
            final long appenderPosition = streamProcessorContext.getTargetStream().getCurrentAppenderPosition();

            if (appenderPosition >= lastWrittenEventPosition)
            {
                final boolean successful = writeSnapshot(lastWrittenEventPosition);
                if (successful)
                {
                    context.take(TRANSITION_DEFAULT);
                }
                else
                {
                    context.take(TRANSITION_FAIL);
                }

                workCount += 1;
            }

            return workCount;
        }

        protected boolean writeSnapshot(final long eventPosition)
        {
            boolean successful = false;
            SnapshotWriter snapshotWriter = null;
            try
            {
                snapshotWriter = snapshotStorage.createSnapshot(streamProcessorContext.getName(), eventPosition);

                snapshotWriter.writeSnapshot(streamProcessorContext.getStateResource());
                snapshotWriter.commit();
                successful = true;
            }
            catch (Exception e)
            {
                e.printStackTrace();

                if (snapshotWriter != null)
                {
                    snapshotWriter.abort();
                }
            }
            return successful;
        }
    }

    private class RecoveringState implements TransitionState<Context>
    {
        @Override
        public void work(Context context)
        {
            try
            {
                streamProcessorContext.getStateResource().reset();

                recoverFromSnapshot();

                context.take(TRANSITION_DEFAULT);
            }
            catch (Exception e)
            {
                e.printStackTrace();

                context.take(TRANSITION_FAIL);
                context.completeFutureExceptionally(e);
            }
        }

        private void recoverFromSnapshot() throws Exception
        {
            final ReadableSnapshot lastSnapshot = snapshotStorage.getLastSnapshot(streamProcessorContext.getName());

            if (lastSnapshot != null)
            {
                // recover last snapshot
                lastSnapshot.recoverFromSnapshot(streamProcessorContext.getStateResource());
                lastSnapshot.validateAndClose();

                // read the last event from snapshot
                final boolean found = targetLogStreamReader.seek(lastSnapshot.getPosition());

                if (found && targetLogStreamReader.hasNext())
                {
                    final LoggedEvent lastEventFromSnapshot = targetLogStreamReader.next();
                    // resume the next position on source log stream to continue from
                    sourceLogStreamReader.seek(lastEventFromSnapshot.getSourceEventPosition() + 1);
                }
                else
                {
                    throw new IllegalStateException("Cannot found event with the snapshot position in target log stream.");
                }
            }
        }
    }

    private class ReprocessingState implements State<Context>
    {
        @Override
        public int doWork(Context context)
        {
            if (targetLogStreamReader.hasNext())
            {
                final LoggedEvent targetEvent = targetLogStreamReader.next();
                try
                {
                    processEvent(targetEvent);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                    context.take(TRANSITION_FAIL);
                    context.completeFutureExceptionally(e);
                }
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

        protected void processEvent(final LoggedEvent targetEvent)
        {
            // ignore events from other producers
            if (targetEvent.getProducerId() == streamProcessorContext.getId())
            {
                final long sourceEventPosition = targetEvent.getSourceEventPosition();

                // assuming that the log stream reader seek to a nearby position before
                LoggedEvent sourceEvent = null;
                long currentSourceEventPosition = -1L;
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
                    throw new IllegalStateException("Cannot find source event of written event: " + targetEvent);
                }
            }
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

                agentRunnerService.remove(StreamProcessorController.this);
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
                context.setFailedEventPosition(failedPosition);
                context.take(TRANSITION_FAIL);
            });
        }

        @Override
        public void onRecovered()
        {
            stateMachineAgent.addCommand(context ->
            {
                final long failedEventPosition = context.getFailedEventPosition();
                if (failedEventPosition > 0 && failedEventPosition <= context.getLastWrittenEventPosition())
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
        private long failedEventPosition = -1;
        private CompletableFuture<Void> future;

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
    }

}
