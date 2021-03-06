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
package io.zeebe.logstreams.processor;

import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.spi.ReadableSnapshot;
import io.zeebe.logstreams.spi.SnapshotStorage;
import io.zeebe.logstreams.spi.SnapshotWriter;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamProcessorController extends Actor
{
    private static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;

    private static final String ERROR_MESSAGE_REPROCESSING_FAILED = "Stream processor '%s' failed to reprocess. Cannot find source event position: %d";
    private static final String ERROR_MESSAGE_PROCESSING_FAILED = "Stream processor '{}' failed to process event. It stop processing further events.";

    private final StreamProcessor streamProcessor;
    private final StreamProcessorContext streamProcessorContext;

    private final LogStreamReader logStreamReader;
    private final LogStreamWriter logStreamWriter;

    private final SnapshotStorage snapshotStorage;
    private final Duration snapshotPeriod;

    private final ActorScheduler actorScheduler;
    private final AtomicBoolean isOpened = new AtomicBoolean(false);
    private final AtomicBoolean isFailed = new AtomicBoolean(false);

    private final EventFilter eventFilter;
    private final EventFilter reprocessingEventFilter;
    private final boolean isReadOnlyProcessor;

    private final Runnable readNextEvent = this::readNextEvent;

    private CompletableActorFuture<Void> openFuture;

    private long snapshotPosition = -1L;
    private long lastSourceEventPosition = -1L;
    private long eventPosition = -1L;
    private long lastSuccessfulProcessedEventPosition = -1L;
    private long lastWrittenEventPosition = -1L;

    private LoggedEvent currentEvent;
    private EventProcessor eventProcessor;
    private ActorCondition onCommitPositionUpdatedCondition;

    private boolean suspended = false;

    public StreamProcessorController(StreamProcessorContext context)
    {
        this.streamProcessorContext = context;
        this.streamProcessorContext.setActorControl(actor);

        this.streamProcessorContext.setSuspendRunnable(this::suspend);
        this.streamProcessorContext.setResumeRunnable(this::resume);

        this.actorScheduler = context.getActorScheduler();
        this.streamProcessor = context.getStreamProcessor();
        this.logStreamReader = context.getLogStreamReader();
        this.logStreamWriter = context.getLogStreamWriter();
        this.snapshotStorage = context.getSnapshotStorage();
        this.snapshotPeriod = context.getSnapshotPeriod();
        this.eventFilter = context.getEventFilter();
        this.reprocessingEventFilter = context.getReprocessingEventFilter();
        this.isReadOnlyProcessor = context.isReadOnlyProcessor();
    }

    @Override
    public String getName()
    {
        return streamProcessorContext.getName();
    }

    public ActorFuture<Void> openAsync()
    {
        if (isOpened.compareAndSet(false, true))
        {
            openFuture = new CompletableActorFuture<>();

            actorScheduler.submitActor(this);

            return openFuture;
        }
        else
        {
            return CompletableActorFuture.completed(null);
        }
    }

    @Override
    protected void onActorStarted()
    {
        final LogStream logStream = streamProcessorContext.getLogStream();

        logStreamReader.wrap(logStream);
        logStreamWriter.wrap(logStream);

        recoverFromSnapshot();
    }

    private void recoverFromSnapshot()
    {
        streamProcessor.getStateResource().reset();

        snapshotPosition = -1;

        try
        {
            final ReadableSnapshot lastSnapshot = snapshotStorage.getLastSnapshot(streamProcessorContext.getName());
            if (lastSnapshot != null)
            {
                // recover last snapshot
                lastSnapshot.recoverFromSnapshot(streamProcessor.getStateResource());

                // read the last event from snapshot
                snapshotPosition = lastSnapshot.getPosition();
                final boolean found = logStreamReader.seek(snapshotPosition);

                if (found && logStreamReader.hasNext())
                {
                    // resume the next position on source log stream to continue from
                    final long sourceEventPosition = snapshotPosition;
                    logStreamReader.seek(sourceEventPosition + 1);
                }
                else
                {
                    throw new IllegalStateException(
                                                    String.format("Stream processor '%s' failed to recover. Cannot find event with the snapshot position in target log stream.",
                                                                  getName()));
                }
            }

            streamProcessor.onOpen(streamProcessorContext);

            seekToLastSourceEvent();
        }
        catch (Exception e)
        {
            openFuture.completeExceptionally(e);

            onFailure();
        }
    }

    private void seekToLastSourceEvent()
    {
        if (!isReadOnlyProcessor && logStreamReader.hasNext())
        {
            final long lastSourceEventPosition = findLastSourceEvent();
            logStreamReader.seek(snapshotPosition + 1);

            if (lastSourceEventPosition > snapshotPosition)
            {
                this.lastSourceEventPosition = lastSourceEventPosition;

                actor.runUntilDone(this::reprocessNextEvent);
            }
            else
            {
                // nothing to reprocess
                onOpened();
            }
        }
        else
        {
            // nothing to reprocess
            onOpened();
        }
    }

    private long findLastSourceEvent()
    {
        long lastSourceEventPosition = snapshotPosition;
        while (logStreamReader.hasNext())
        {
            final LoggedEvent newEvent = logStreamReader.next();

            // ignore events from other producers
            if (newEvent.getProducerId() == streamProcessorContext.getId()
                && ((reprocessingEventFilter == null || reprocessingEventFilter.applies(newEvent))))
            {
                final long sourceEventPosition = newEvent.getSourceEventPosition();
                if (sourceEventPosition > 0 && sourceEventPosition > lastSourceEventPosition)
                {
                    lastSourceEventPosition = sourceEventPosition;
                }

            }
        }
        return lastSourceEventPosition;
    }

    private void reprocessNextEvent()
    {
        try
        {
            if (logStreamReader.hasNext())
            {
                final LoggedEvent currentEvent = logStreamReader.next();
                final long currentEventPosition = currentEvent.getPosition();

                if (currentEventPosition <= lastSourceEventPosition)
                {
                    reprocessEvent(currentEvent);

                    if (currentEventPosition == lastSourceEventPosition)
                    {
                        // all events are re-processed
                        actor.done();

                        onOpened();
                    }
                    else
                    {
                        // continue with next event
                        actor.yield();
                    }
                }
                else
                {
                    throw new IllegalStateException(
                                                    String.format(ERROR_MESSAGE_REPROCESSING_FAILED,
                                                                  streamProcessorContext.getName(),
                                                                  lastSourceEventPosition));
                }
            }
            else
            {
                throw new IllegalStateException(
                                                String.format(ERROR_MESSAGE_REPROCESSING_FAILED,
                                                              streamProcessorContext.getName(),
                                                              lastSourceEventPosition));
            }
        }
        catch (Exception e)
        {
            actor.done();

            openFuture.completeExceptionally(e);

            onFailure();
        }
    }

    private void reprocessEvent(LoggedEvent currentEvent)
    {
        if (eventFilter == null || eventFilter.applies(currentEvent))
        {
            try
            {
                final EventProcessor eventProcessor = streamProcessor.onEvent(currentEvent);

                if (eventProcessor != null)
                {
                    eventProcessor.processEvent();
                    eventProcessor.updateState();
                    streamProcessor.afterEvent();
                }
            }
            catch (Exception e)
            {
                final String errorMessage = "Stream processor '%s' failed to reprocess event: %s";
                throw new RuntimeException(String.format(errorMessage, streamProcessorContext.getName(), currentEvent, e));
            }
        }
    }

    private void onOpened()
    {
        if (openFuture != null)
        {
            openFuture.complete(null);
        }

        onCommitPositionUpdatedCondition = actor.onCondition(getName() + "-on-commit-position-updated", readNextEvent);
        streamProcessorContext.logStream.registerOnCommitPositionUpdatedCondition(onCommitPositionUpdatedCondition);

        // start reading
        actor.run(readNextEvent);

        actor.runAtFixedRate(snapshotPeriod, this::createSnapshot);
    }

    private void readNextEvent()
    {
        if (isOpened() && !isSuspended() && logStreamReader.hasNext())
        {
            final LoggedEvent event = logStreamReader.next();
            currentEvent = event;

            if (eventFilter == null || eventFilter.applies(event))
            {
                processEvent(currentEvent);
            }
            else
            {
                // continue with the next event
                actor.submit(readNextEvent);
            }
        }
        else
        {
            actor.yield();
        }
    }

    private void processEvent(LoggedEvent event)
    {
        eventProcessor = streamProcessor.onEvent(event);

        if (eventProcessor != null)
        {
            try
            {
                eventProcessor.processEvent();

                actor.runUntilDone(this::executeSideEffects);
            }
            catch (Exception e)
            {
                LOG.error(ERROR_MESSAGE_PROCESSING_FAILED, getName(), e);
                onFailure();
            }
        }
        else
        {
            // continue with the next event
            actor.submit(readNextEvent);
        }
    }

    private void executeSideEffects()
    {
        try
        {
            final boolean success = eventProcessor.executeSideEffects();
            if (success)
            {
                actor.done();

                actor.runUntilDone(this::writeEvent);
            }
            else if (isOpened())
            {
                // try again
                actor.yield();
            }
            else
            {
                actor.done();
            }
        }
        catch (Exception e)
        {
            LOG.error(ERROR_MESSAGE_PROCESSING_FAILED, getName(), e);

            actor.done();
            onFailure();
        }
    }

    private void writeEvent()
    {
        try
        {
            final LogStream sourceStream = streamProcessorContext.getLogStream();

            logStreamWriter
                .producerId(streamProcessorContext.getId())
                .sourceEvent(sourceStream.getPartitionId(), currentEvent.getPosition());

            eventPosition = eventProcessor.writeEvent(logStreamWriter);

            if (eventPosition >= 0)
            {
                actor.done();

                updateState();
            }
            else if (isOpened())
            {
                // try again
                actor.yield();
            }
            else
            {
                actor.done();
            }
        }
        catch (Exception e)
        {
            LOG.error(ERROR_MESSAGE_PROCESSING_FAILED, getName(), e);

            actor.done();
            onFailure();
        }
    }

    private void updateState()
    {
        try
        {
            eventProcessor.updateState();
            streamProcessor.afterEvent();

            lastSuccessfulProcessedEventPosition = currentEvent.getPosition();

            final boolean hasWrittenEvent = eventPosition > 0;
            if (hasWrittenEvent)
            {
                lastWrittenEventPosition = eventPosition;
            }

            // continue with next event
            actor.submit(readNextEvent);
        }
        catch (Exception e)
        {
            LOG.error(ERROR_MESSAGE_PROCESSING_FAILED, getName(), e);
            onFailure();
        }
    }

    private void createSnapshot()
    {
        if (currentEvent != null)
        {
            final long commitPosition = streamProcessorContext.getLogStream().getCommitPosition();

            final boolean snapshotAlreadyPresent = lastSuccessfulProcessedEventPosition <= snapshotPosition;

            if (!snapshotAlreadyPresent)
            {
                // ensure that the last written event was committed
                if (commitPosition >= lastWrittenEventPosition)
                {
                    writeSnapshot(lastSuccessfulProcessedEventPosition);
                }
            }
        }
    }

    protected void writeSnapshot(final long eventPosition)
    {
        SnapshotWriter snapshotWriter = null;
        try
        {
            final long start = System.currentTimeMillis();
            final String name = streamProcessorContext.getName();
            LOG.info("Write snapshot for stream processor {} at event position {}.", name, eventPosition);

            snapshotWriter = snapshotStorage.createSnapshot(name, eventPosition);

            snapshotWriter.writeSnapshot(streamProcessor.getStateResource());
            snapshotWriter.commit();
            LOG.info("Creation of snapshot {} took {} ms.", name, System.currentTimeMillis() - start);

            snapshotPosition = eventPosition;
        }
        catch (Exception e)
        {
            LOG.error("Stream processor '{}' failed. Can not write snapshot.", getName(), e);

            if (snapshotWriter != null)
            {
                snapshotWriter.abort();
            }
        }
    }

    public ActorFuture<Void> closeAsync()
    {
        if (isOpened.compareAndSet(true, false))
        {
            return actor.close();
        }
        else
        {
            return CompletableActorFuture.completed(null);
        }
    }

    @Override
    protected void onActorClosing()
    {
        if (!isFailed())
        {
            createSnapshot();
            streamProcessor.onClose();
        }

        streamProcessorContext.getLogStreamReader().close();

        streamProcessorContext.logStream.removeOnCommitPositionUpdatedCondition(onCommitPositionUpdatedCondition);
        onCommitPositionUpdatedCondition = null;
    }

    private void onFailure()
    {
        if (isFailed.compareAndSet(false, true))
        {
            isOpened.set(false);

            actor.close();
        }
    }

    public boolean isOpened()
    {
        return isOpened.get();
    }

    public boolean isFailed()
    {
        return isFailed.get();
    }

    public EventFilter getEventFilter()
    {
        return eventFilter;
    }

    public EventFilter getReprocessingEventFilter()
    {
        return reprocessingEventFilter;
    }

    public boolean isSuspended()
    {
        return suspended;
    }

    private void suspend()
    {
        suspended = true;
    }

    private void resume()
    {
        suspended = false;
        actor.submit(readNextEvent);
    }
}
