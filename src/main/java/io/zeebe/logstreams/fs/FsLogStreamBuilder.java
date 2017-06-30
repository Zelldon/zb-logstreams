package io.zeebe.logstreams.fs;

import org.agrona.DirectBuffer;
import io.zeebe.logstreams.impl.LogStreamImpl;
import io.zeebe.logstreams.impl.log.fs.FsLogStorage;
import io.zeebe.logstreams.impl.log.fs.FsLogStorageConfiguration;
import io.zeebe.logstreams.spi.SnapshotStorage;

import java.io.File;

/**
 * @author Christopher Zell <christopher.zell@camunda.com>
 */
public class FsLogStreamBuilder extends LogStreamImpl.LogStreamBuilder<FsLogStreamBuilder>
{
    public FsLogStreamBuilder(final DirectBuffer topicName, final int partitionId)
    {
        super(topicName, partitionId);
    }

    @Override
    protected void initLogStorage()
    {
        if (logDirectory == null)
        {
            logDirectory = logRootPath + File.separatorChar + logName + File.separatorChar;
        }

        final File file = new File(logDirectory);
        file.mkdirs();

        final FsLogStorageConfiguration storageConfig = new FsLogStorageConfiguration(logSegmentSize,
            logDirectory,
            initialLogSegmentId,
            deleteOnClose);

        logStorage = new FsLogStorage(storageConfig);
        logStorage.open();
    }

    @Override
    public SnapshotStorage getSnapshotStorage()
    {
        if (snapshotStorage == null)
        {
            snapshotStorage = new FsSnapshotStorageBuilder(logDirectory).build();
        }
        return snapshotStorage;
    }
}