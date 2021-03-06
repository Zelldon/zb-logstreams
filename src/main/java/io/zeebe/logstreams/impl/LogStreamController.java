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
package io.zeebe.logstreams.impl;

import static io.zeebe.dispatcher.impl.log.DataFrameDescriptor.messageOffset;
import static io.zeebe.logstreams.impl.LogEntryDescriptor.positionOffset;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import io.zeebe.dispatcher.*;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.util.sched.*;
import io.zeebe.util.sched.channel.ActorConditions;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.agrona.MutableDirectBuffer;
import org.slf4j.Logger;

public class LogStreamController extends Actor
{
    public static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;

    private final AtomicBoolean isOpenend = new AtomicBoolean(false);
    private final AtomicBoolean isFailed = new AtomicBoolean(false);

    private final ActorConditions onLogStorageAppendedConditions;

    private Runnable peekedBlockHandler;

    private long firstEventPosition;

    private CompletableActorFuture<Void> openFuture;

    //  MANDATORY //////////////////////////////////////////////////
    private String name;
    private LogStorage logStorage;
    private ActorScheduler actorScheduler;

    private final BlockPeek blockPeek = new BlockPeek();
    private int maxAppendBlockSize;
    private Dispatcher writeBuffer;
    private Subscription writeBufferSubscription;

    public LogStreamController(LogStreamImpl.LogStreamBuilder logStreamBuilder, ActorConditions onLogStorageAppendedConditions)
    {
        wrap(logStreamBuilder);

        this.onLogStorageAppendedConditions = onLogStorageAppendedConditions;
    }

    protected void wrap(LogStreamImpl.LogStreamBuilder logStreamBuilder)
    {
        this.name = logStreamBuilder.getLogName() + ".appender";
        this.logStorage = logStreamBuilder.getLogStorage();
        this.actorScheduler = logStreamBuilder.getActorScheduler();

        this.maxAppendBlockSize = logStreamBuilder.getMaxAppendBlockSize();
        this.writeBuffer = logStreamBuilder.getWriteBuffer();
    }

    @Override
    public String getName()
    {
        return name;
    }

    public void open()
    {
        openAsync().join();
    }

    public ActorFuture<Void> openAsync()
    {
        if (isOpenend.compareAndSet(false, true))
        {
            final CompletableActorFuture<Void> openFuture = new CompletableActorFuture<>();
            this.openFuture = openFuture;

            actorScheduler.submitActor(this, true, SchedulingHints.ioBound((short) 0));

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
        if (!logStorage.isOpen())
        {
            logStorage.open();
        }

        actor.runOnCompletion(writeBuffer.getSubscriptionAsync("log-appender"), (subscription, failure) ->
        {
            if (failure == null)
            {
                writeBufferSubscription = subscription;

                peekedBlockHandler = this::appendBlock;
                actor.consume(writeBufferSubscription, this::peek);

                openFuture.complete(null);
                openFuture = null;
            }
            else
            {
                openFuture.completeExceptionally(failure);
                openFuture = null;
            }
        });
    }

    private void peek()
    {
        if (writeBufferSubscription.peekBlock(blockPeek, maxAppendBlockSize, true) > 0)
        {
            peekedBlockHandler.run();
        }
        else
        {
            actor.yield();
        }
    }

    private void appendBlock()
    {
        final ByteBuffer nioBuffer = blockPeek.getRawBuffer();
        final MutableDirectBuffer buffer = blockPeek.getBuffer();

        final long position = buffer.getLong(positionOffset(messageOffset(0)));
        firstEventPosition = position;

        final long address = logStorage.append(nioBuffer);

        if (address >= 0)
        {
            blockPeek.markCompleted();

            onLogStorageAppendedConditions.signalConsumers();
        }
        else
        {
            isFailed.set(true);

            LOG.debug("Failed to append log storage on position '{}'. Discard the following blocks.", firstEventPosition);

            peekedBlockHandler = this::discardBlock;

            discardBlock();
        }
    }

    private void discardBlock()
    {
        blockPeek.markFailed();
        // continue with next block
        actor.yield();
    }

    public void close()
    {
        closeAsync().join();
    }

    public ActorFuture<Void> closeAsync()
    {
        if (isOpenend.compareAndSet(true, false))
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
        isOpenend.set(false);
        isFailed.set(false);
    }

    public boolean isOpened()
    {
        return isOpenend.get();
    }

    public boolean isClosed()
    {
        return !isOpenend.get();
    }

    public boolean isFailed()
    {
        return isFailed.get();
    }

    public long getCurrentAppenderPosition()
    {
        if (writeBufferSubscription != null)
        {
            return writeBufferSubscription.getPosition();
        }
        else
        {
            return -1L;
        }
    }

    protected int getMaxAppendBlockSize()
    {
        return maxAppendBlockSize;
    }
}
