package org.camunda.tngp.logstreams.impl.fs;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;

import org.agrona.BitUtil;
import org.agrona.LangUtil;
import org.camunda.tngp.logstreams.spi.SnapshotWriter;
import org.camunda.tngp.util.FileUtil;

public class FsSnapshotWriter implements SnapshotWriter
{
    protected final FsSnapshotStorageConfiguration config;
    protected final File dataFile;
    protected final File checksumFile;
    protected final FsReadableSnapshot lastSnapshot;

    protected DigestOutputStream dataOutputStream;
    protected BufferedOutputStream checksumOutputStream;

    public FsSnapshotWriter(FsSnapshotStorageConfiguration config, File snapshotFile, File checksumFile, FsReadableSnapshot lastSnapshot)
    {
        this.config = config;
        this.dataFile = snapshotFile;
        this.checksumFile = checksumFile;
        this.lastSnapshot = lastSnapshot;

        initOutputStreams(config, snapshotFile, checksumFile);
    }

    protected void initOutputStreams(FsSnapshotStorageConfiguration config, File snapshotFile, File checksumFile)
    {
        try
        {
            final MessageDigest messageDigest = MessageDigest.getInstance(config.getChecksumAlgorithm());

            final FileOutputStream dataFileOutputStream = new FileOutputStream(snapshotFile);
            final BufferedOutputStream bufferedDataOutputStream = new BufferedOutputStream(dataFileOutputStream);
            dataOutputStream = new DigestOutputStream(bufferedDataOutputStream, messageDigest);

            final FileOutputStream checksumFileOutputStream = new FileOutputStream(checksumFile);
            checksumOutputStream = new BufferedOutputStream(checksumFileOutputStream);

        }
        catch (Exception e)
        {
            abort();
            LangUtil.rethrowUnchecked(e);
        }
    }

    @Override
    public OutputStream getOutputStream()
    {
        return dataOutputStream;
    }

    @Override
    public void commit() throws Exception
    {
        try
        {
            dataOutputStream.close();

            final MessageDigest digest = dataOutputStream.getMessageDigest();

            final byte[] digestBytes = digest.digest();
            final String digestString = BitUtil.toHex(digestBytes);
            final String checksumFileContents = config.checksumContent(digestString, dataFile.getName());

            checksumOutputStream.write(checksumFileContents.getBytes(StandardCharsets.UTF_8));
            checksumOutputStream.close();

            if (lastSnapshot != null)
            {
                // TODO some how the data file can't be deleted because of another process
                lastSnapshot.getDataFile().delete();
                lastSnapshot.getChecksumFile().delete();
            }
        }
        catch (Exception e)
        {
            abort();
            LangUtil.rethrowUnchecked(e);
        }
    }

    @Override
    public void abort()
    {
        FileUtil.closeSilently(dataOutputStream);
        FileUtil.closeSilently(checksumOutputStream);
        dataFile.delete();
        checksumFile.delete();
    }

    public File getChecksumFile()
    {
        return checksumFile;
    }

    public File getDataFile()
    {
        return dataFile;
    }
}
