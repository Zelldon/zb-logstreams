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
package org.camunda.tngp.logstreams.fs.log;

import static org.assertj.core.api.Assertions.*;
import static org.camunda.tngp.dispatcher.impl.PositionUtil.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import org.camunda.tngp.dispatcher.impl.PositionUtil;
import org.camunda.tngp.logstreams.impl.log.fs.FsLogSegmentDescriptor;
import org.camunda.tngp.logstreams.impl.log.fs.FsLogStorage;
import org.camunda.tngp.logstreams.impl.log.fs.FsLogStorageConfiguration;
import org.camunda.tngp.logstreams.spi.LogStorage;
import org.camunda.tngp.util.FileUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class FsLogStorageTest
{
    private static final int SEGMENT_SIZE = 1024 * 16;

    private static final byte[] MSG = "test".getBytes();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private String logPath;
    private File logDirectory;

    private FsLogStorageConfiguration fsStorageConfig;

    private FsLogStorage fsLogStorage;

    @Before
    public void init()
    {
        logPath = tempFolder.getRoot().getAbsolutePath();
        logDirectory = new File(logPath);

        fsStorageConfig = new FsLogStorageConfiguration(SEGMENT_SIZE, logPath, 0, false);

        fsLogStorage = new FsLogStorage(fsStorageConfig);
    }

    @Test
    public void shouldGetConfig()
    {
        assertThat(fsLogStorage.getConfig()).isEqualTo(fsStorageConfig);
    }

    @Test
    public void shouldBeByteAddressable()
    {
        assertThat(fsLogStorage.isByteAddressable()).isTrue();
    }

    @Test
    public void shouldGetFirstBlockAddressIfEmpty()
    {
        fsLogStorage.open();

        assertThat(fsLogStorage.getFirstBlockAddress()).isEqualTo(-1);
    }

    @Test
    public void shouldGetFirstBlockAddressIfExists()
    {
        fsLogStorage.open();

        final long address = fsLogStorage.append(ByteBuffer.wrap(MSG));

        assertThat(fsLogStorage.getFirstBlockAddress()).isEqualTo(address);
    }

    @Test
    public void shouldNotGetFirstBlockAddressIfNotOpen()
    {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("log storage is not open");

        fsLogStorage.getFirstBlockAddress();
    }

    @Test
    public void shouldNotGetFirstBlockAddressIfClosed()
    {
        fsLogStorage.open();
        fsLogStorage.close();

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("log storage is already closed");

        fsLogStorage.getFirstBlockAddress();
    }

    @Test
    public void shouldCreateLogOnOpenStorage()
    {
        final String initialSegmentFilePath = fsStorageConfig.fileName(fsStorageConfig.getInitialSegmentId());

        fsLogStorage.open();

        final File[] files = logDirectory.listFiles();

        assertThat(files).hasSize(1);
        assertThat(files[0].getAbsolutePath()).isEqualTo(initialSegmentFilePath);
    }

    @Test
    public void shouldNotDeleteLogOnCloseStorage()
    {
        fsLogStorage.open();

        fsLogStorage.close();

        assertThat(logDirectory).exists();
    }

    @Test
    public void shouldDeleteLogOnCloseStorage()
    {
        fsStorageConfig = new FsLogStorageConfiguration(SEGMENT_SIZE, logPath, 0, true);
        fsLogStorage = new FsLogStorage(fsStorageConfig);

        fsLogStorage.open();

        fsLogStorage.close();

        assertThat(logDirectory).doesNotExist();
    }

    @Test
    public void shouldAppendBlock()
    {
        fsLogStorage.open();

        final long address = fsLogStorage.append(ByteBuffer.wrap(MSG));

        assertThat(address).isGreaterThan(0);

        final byte[] writtenBytes = readLogFile(fsStorageConfig.fileName(0), address, MSG.length);
        assertThat(writtenBytes).isEqualTo(MSG);
    }

    @Test
    public void shouldAppendBlockOnNextSegment()
    {
        fsLogStorage.open();
        fsLogStorage.append(ByteBuffer.wrap(MSG));

        assertThat(logDirectory.listFiles().length).isEqualTo(1);

        final int remainingCapacity = SEGMENT_SIZE - FsLogSegmentDescriptor.METADATA_LENGTH - MSG.length;
        final byte[] largeBlock = new byte[remainingCapacity + 1];
        new Random().nextBytes(largeBlock);

        final long address = fsLogStorage.append(ByteBuffer.wrap(largeBlock));

        assertThat(address).isGreaterThan(0);
        assertThat(logDirectory.listFiles().length).isEqualTo(2);

        final byte[] writtenBytes = readLogFile(fsStorageConfig.fileName(1), partitionOffset(address), largeBlock.length);
        assertThat(writtenBytes).isEqualTo(largeBlock);

        fsLogStorage.close();
    }

    @Test
    public void shouldNotAppendBlockIfSizeIsGreaterThanSegment()
    {
        final byte[] largeBlock = new byte[SEGMENT_SIZE + 1];
        new Random().nextBytes(largeBlock);

        fsLogStorage.open();

        final long result = fsLogStorage.append(ByteBuffer.wrap(largeBlock));

        assertThat(result).isEqualTo(LogStorage.OP_RESULT_BLOCK_SIZE_TOO_BIG);
    }

    @Test
    public void shouldNotAppendBlockIfNotOpen()
    {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("log storage is not open");

        fsLogStorage.append(ByteBuffer.wrap(MSG));
    }

    @Test
    public void shouldNotAppendBlockIfClosed()
    {
        fsLogStorage.open();
        fsLogStorage.close();

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("log storage is already closed");

        fsLogStorage.append(ByteBuffer.wrap(MSG));
    }

    @Test
    public void shouldReadAppendedBlock()
    {
        final ByteBuffer readBuffer = ByteBuffer.allocate(MSG.length);

        fsLogStorage.open();

        final long address = fsLogStorage.append(ByteBuffer.wrap(MSG));

        final long result = fsLogStorage.read(readBuffer, address);

        assertThat(result).isEqualTo(address + MSG.length);
        assertThat(readBuffer.array()).isEqualTo(MSG);
    }

    @Test
    public void shouldNotReadBlockIfAddressIsInvalid()
    {
        final ByteBuffer readBuffer = ByteBuffer.allocate(MSG.length);

        fsLogStorage.open();

        final long result = fsLogStorage.read(readBuffer, -1);

        assertThat(result).isEqualTo(LogStorage.OP_RESULT_INVALID_ADDR);
    }

    @Test
    public void shouldNotReadBlockIfNotAvailable()
    {
        final ByteBuffer readBuffer = ByteBuffer.allocate(MSG.length);

        fsLogStorage.open();

        final long address = fsLogStorage.append(ByteBuffer.wrap(MSG));
        final long nextAddress = fsLogStorage.read(readBuffer, address);

        final long result = fsLogStorage.read(readBuffer, nextAddress);

        assertThat(result).isEqualTo(LogStorage.OP_RESULT_NO_DATA);
    }

    @Test
    public void shouldNotReadBlockIfNotOpen()
    {
        final ByteBuffer readBuffer = ByteBuffer.allocate(MSG.length);

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("log storage is not open");

        fsLogStorage.read(readBuffer, 0);
    }

    @Test
    public void shouldNotReadBlockIfClosed()
    {
        final ByteBuffer readBuffer = ByteBuffer.allocate(MSG.length);

        fsLogStorage.open();

        final long address = fsLogStorage.append(ByteBuffer.wrap(MSG));

        fsLogStorage.close();

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("log storage is already closed");

        fsLogStorage.read(readBuffer, address);
    }

    @Test
    public void shouldRestoreLogOnReOpenedStorage()
    {
        final ByteBuffer readBuffer = ByteBuffer.allocate(MSG.length);

        fsLogStorage.open();

        final long address = fsLogStorage.append(ByteBuffer.wrap(MSG));

        fsLogStorage.close();

        fsLogStorage.open();

        assertThat(fsLogStorage.getFirstBlockAddress()).isEqualTo(address);

        fsLogStorage.read(readBuffer, address);

        assertThat(readBuffer.array()).isEqualTo(MSG);
    }

    @Test
    public void shouldRepairSegmentIfInconsistentOnOpen() throws IOException
    {
        // append the log storage
        fsLogStorage.open();
        fsLogStorage.append(ByteBuffer.wrap(MSG));
        fsLogStorage.close();

        try (FileChannel fileChannel = FileUtil.openChannel(fsStorageConfig.fileName(0), false))
        {
            final long originalFileSize = fileChannel.size();

            // append the underlying file
            fileChannel.position(originalFileSize);
            fileChannel.write(ByteBuffer.wrap("foo".getBytes()));

            assertThat(fileChannel.size()).isGreaterThan(originalFileSize);

            // open the log storage to trigger auto-repair
            fsLogStorage.open();

            // verify that the log storage is restored
            assertThat(fileChannel.size()).isEqualTo(originalFileSize);
        }
    }

    @Test
    public void shouldFailIfSegmentIsInconsistentOnOpen() throws IOException
    {
        // append the log storage
        fsLogStorage.open();
        fsLogStorage.append(ByteBuffer.wrap(MSG));
        fsLogStorage.close();

        try (FileChannel fileChannel = FileUtil.openChannel(fsStorageConfig.fileName(0), false))
        {
            final long fileSize = fileChannel.size();

            // remove bytes of the underlying file
            fileChannel.truncate(fileSize - 1);

            thrown.expect(RuntimeException.class);
            thrown.expectMessage("Inconsistent log segment");

            fsLogStorage.open();
        }
    }

    @Test
    public void shouldNotTruncateIfNotOpen()
    {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("log storage is not open");

        fsLogStorage.truncate(0);
    }

    @Test
    public void shouldNotTruncateIfClosed()
    {
        fsLogStorage.open();

        fsLogStorage.append(ByteBuffer.wrap(MSG));

        fsLogStorage.close();

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("log storage is already closed");

        fsLogStorage.truncate(0);
    }

    @Test
    public void shouldNotTruncateIfGivenSegmentIsLessThanInitialSegment()
    {
        fsLogStorage.open();

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Cannot truncate log: invalid address");

        final long addr = PositionUtil.position(-1, 0);
        fsLogStorage.truncate(addr);
    }

    @Test
    public void shouldNotTruncateIfGivenSegmentIsGreaterThanCurrentSegment()
    {
        fsLogStorage.open();

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Cannot truncate log: invalid address");

        final long addr = PositionUtil.position(1, 0);
        fsLogStorage.truncate(addr);
    }

    @Test
    public void shouldNotTruncateIfGivenOffsetIsLessThanMetadataLength()
    {
        fsLogStorage.open();

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Cannot truncate log: invalid address");

        final long addr = PositionUtil.position(0, 0);
        fsLogStorage.truncate(addr);
    }

    @Test
    public void shouldNotTruncateIfGivenOffsetIsEqualToCurrentSize()
    {
        fsLogStorage.open();
        final long address = fsLogStorage.append(ByteBuffer.wrap(MSG));
        final int offset = PositionUtil.partitionOffset(address);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Cannot truncate log: invalid address");

        final long addr = PositionUtil.position(0, offset + MSG.length);
        fsLogStorage.truncate(addr);
    }

    @Test
    public void shouldNotTruncateIfGivenOffsetIsGreaterThanCurrentSize()
    {
        fsLogStorage.open();
        final long address = fsLogStorage.append(ByteBuffer.wrap(MSG));
        final int offset = PositionUtil.partitionOffset(address);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Cannot truncate log: invalid address");

        final long addr = PositionUtil.position(0, offset + MSG.length + 1);
        fsLogStorage.truncate(addr);
    }

    @Test
    public void shouldRemoveBakFilesWhenOpeningStorage() throws Exception
    {
        fsLogStorage.open();
        fsLogStorage.close();

        final String initialSegmentFileName = fsStorageConfig.fileName(fsStorageConfig.getInitialSegmentId());
        final String initialSegmentBakFileName = initialSegmentFileName + ".bak";

        final Path initialSegmentPath = Paths.get(initialSegmentFileName);
        final Path initialSegmentBakPath = Paths.get(initialSegmentBakFileName);

        Files.copy(initialSegmentPath, initialSegmentBakPath);
        assertThat(logDirectory.listFiles().length).isEqualTo(2);

        // when
        fsLogStorage.open();

        assertThat(logDirectory.listFiles().length).isEqualTo(1);
        assertThat(logDirectory.listFiles()[0].getPath()).doesNotEndWith(".bak");
    }

    @Test
    public void shouldApplyTruncatedInitialSegment() throws Exception
    {
        fsLogStorage.open();
        final long address = fsLogStorage.append(ByteBuffer.wrap(MSG));
        fsLogStorage.close();

        final String initialSegmentFileName = fsStorageConfig.fileName(fsStorageConfig.getInitialSegmentId());
        final String initialSegmentTruncatedFileName = initialSegmentFileName + ".bak.truncated";

        final Path initialSegmentPath = Paths.get(initialSegmentFileName);
        final Path initialSegmentBakPath = Paths.get(initialSegmentTruncatedFileName);

        Files.copy(initialSegmentPath, initialSegmentBakPath);
        Files.delete(initialSegmentPath);

        assertThat(logDirectory.listFiles().length).isEqualTo(1);

        // when
        fsLogStorage.open();

        // then
        final ByteBuffer readBuffer = ByteBuffer.allocate(MSG.length);
        final long result = fsLogStorage.read(readBuffer, address);
        assertThat(result).isEqualTo(address + MSG.length);
        assertThat(readBuffer.array()).isEqualTo(MSG);
    }

    @Test
    public void shouldApplyTruncatedNextSegment() throws Exception
    {
        fsLogStorage.open();

        final int remainingCapacity = SEGMENT_SIZE - FsLogSegmentDescriptor.METADATA_LENGTH;
        final byte[] largeBlock = new byte[remainingCapacity];

        new Random().nextBytes(largeBlock);
        fsLogStorage.append(ByteBuffer.wrap(largeBlock));

        final long address = fsLogStorage.append(ByteBuffer.wrap(MSG));

        fsLogStorage.close();

        final String nextSegmentFileName = fsStorageConfig.fileName(fsStorageConfig.getInitialSegmentId() + 1);
        final String nextSegmentTruncatedFileName = nextSegmentFileName + ".bak.truncated";

        final Path nextSegmentPath = Paths.get(nextSegmentFileName);
        final Path nextSegmentBakPath = Paths.get(nextSegmentTruncatedFileName);

        Files.copy(nextSegmentPath, nextSegmentBakPath);
        Files.delete(nextSegmentPath);

        // when
        fsLogStorage.open();

        // then
        final String[] files = logDirectory.list();
        for (int i = 0; i < files.length; i++)
        {
            assertThat(files[i]).doesNotEndWith(".bak.truncated");
        }

        final ByteBuffer readBuffer = ByteBuffer.allocate(MSG.length);
        final long result = fsLogStorage.read(readBuffer, address);
        assertThat(result).isEqualTo(address + MSG.length);
        assertThat(readBuffer.array()).isEqualTo(MSG);
    }

    @Test
    public void shouldNotApplyTruncatedSegment() throws Exception
    {
        fsLogStorage.open();
        final long address = fsLogStorage.append(ByteBuffer.wrap(MSG));
        fsLogStorage.close();

        final String initialSegmentFileName = fsStorageConfig.fileName(fsStorageConfig.getInitialSegmentId());
        final String initialSegmentTruncatedFileName = initialSegmentFileName + ".bak.truncated";

        final Path initialSegmentPath = Paths.get(initialSegmentFileName);
        final Path initialSegmentBakPath = Paths.get(initialSegmentTruncatedFileName);

        Files.copy(initialSegmentPath, initialSegmentBakPath);

        // when
        fsLogStorage.open();

        // then
        assertThat(logDirectory.listFiles().length).isEqualTo(1);
        assertThat(logDirectory.listFiles()[0].getPath()).doesNotEndWith(".bak.truncated");

        final ByteBuffer readBuffer = ByteBuffer.allocate(MSG.length);
        final long result = fsLogStorage.read(readBuffer, address);
        assertThat(result).isEqualTo(address + MSG.length);
        assertThat(readBuffer.array()).isEqualTo(MSG);
    }

    @Test
    public void shouldRemoveTruncatedInitialSegment() throws Exception
    {
        fsLogStorage.open();
        final long address = fsLogStorage.append(ByteBuffer.wrap(MSG));
        fsLogStorage.close();

        final String initialSegmentFileName = fsStorageConfig.fileName(fsStorageConfig.getInitialSegmentId());
        final String initialSegmentTruncatedFileName = initialSegmentFileName + ".bak.truncated";

        final Path initialSegmentPath = Paths.get(initialSegmentFileName);
        final Path initialSegmentBakPath = Paths.get(initialSegmentTruncatedFileName);

        Files.copy(initialSegmentPath, initialSegmentBakPath);
        Files.delete(initialSegmentPath);

        assertThat(logDirectory.listFiles().length).isEqualTo(1);

        // when
        fsLogStorage.open();

        // then
        assertThat(logDirectory.listFiles().length).isEqualTo(1);
        assertThat(logDirectory.listFiles()[0].getPath()).doesNotEndWith(".bak.truncated");

        final ByteBuffer readBuffer = ByteBuffer.allocate(MSG.length);
        final long result = fsLogStorage.read(readBuffer, address);
        assertThat(result).isEqualTo(address + MSG.length);
        assertThat(readBuffer.array()).isEqualTo(MSG);
    }

    @Test
    public void shouldThrowExceptionWhenMultipleTruncatedFilesDetected() throws Exception
    {
        fsLogStorage.open();

        final int remainingCapacity = SEGMENT_SIZE - FsLogSegmentDescriptor.METADATA_LENGTH;
        final byte[] largeBlock = new byte[remainingCapacity];

        // segment: 0
        new Random().nextBytes(largeBlock);
        fsLogStorage.append(ByteBuffer.wrap(largeBlock));

        // segment: 1
        new Random().nextBytes(largeBlock);
        fsLogStorage.append(ByteBuffer.wrap(largeBlock));

        fsLogStorage.close();

        final String initialSegmentFileName = fsStorageConfig.fileName(fsStorageConfig.getInitialSegmentId());
        final String initialSegmentTruncatedFileName = initialSegmentFileName + ".bak.truncated";

        final Path initialSegmentPath = Paths.get(initialSegmentFileName);
        final Path initialSegmentBakPath = Paths.get(initialSegmentTruncatedFileName);

        Files.copy(initialSegmentPath, initialSegmentBakPath);

        final String nextSegmentFileName = fsStorageConfig.fileName(fsStorageConfig.getInitialSegmentId() + 1);
        final String nextSegmentTruncatedFileName = nextSegmentFileName + ".bak.truncated";

        final Path nextSegmentPath = Paths.get(nextSegmentFileName);
        final Path nextSegmentBakPath = Paths.get(nextSegmentTruncatedFileName);

        Files.copy(nextSegmentPath, nextSegmentBakPath);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Cannot open log storage: multiple truncated files detected");

        fsLogStorage.open();
    }

    @Test
    public void shouldTruncateLastEntryInSameSegment()
    {
        final ByteBuffer readBuffer = ByteBuffer.allocate(MSG.length);

        fsLogStorage.open();

        final long firstEntry = fsLogStorage.append(ByteBuffer.wrap(MSG));
        final long secondEntry = fsLogStorage.append(ByteBuffer.wrap(MSG));

        fsLogStorage.truncate(secondEntry);

        final long result = fsLogStorage.read(readBuffer, firstEntry);
        assertThat(result).isEqualTo(firstEntry + MSG.length);
        assertThat(readBuffer.array()).isEqualTo(MSG);

        assertThat(fsLogStorage.read(readBuffer, secondEntry)).isEqualTo(LogStorage.OP_RESULT_NO_DATA);
    }

    @Test
    public void shouldTruncateUpToAddress()
    {
        final ByteBuffer readBuffer = ByteBuffer.allocate(MSG.length);

        fsLogStorage.open();

        assertThat(logDirectory.listFiles().length).isEqualTo(1);

        final int remainingCapacity = SEGMENT_SIZE - FsLogSegmentDescriptor.METADATA_LENGTH;
        final byte[] largeBlock = new byte[remainingCapacity];

        // segment: 0
        new Random().nextBytes(largeBlock);
        long address = fsLogStorage.append(ByteBuffer.wrap(largeBlock));
        assertThat(address).isGreaterThan(0);

        // segment: 1
        new Random().nextBytes(largeBlock);
        address = fsLogStorage.append(ByteBuffer.wrap(largeBlock));
        assertThat(address).isGreaterThan(0);

        // segment: 2
        new Random().nextBytes(largeBlock);
        address = fsLogStorage.append(ByteBuffer.wrap(largeBlock));
        assertThat(address).isGreaterThan(0);

        // segment: 3
        final long addressMessage = fsLogStorage.append(ByteBuffer.wrap(MSG));
        assertThat(addressMessage).isGreaterThan(0);

        final byte[] largeBlockAfterMessage = new byte[SEGMENT_SIZE - FsLogSegmentDescriptor.METADATA_LENGTH - MSG.length];
        new Random().nextBytes(largeBlockAfterMessage);
        final long addressTruncate = fsLogStorage.append(ByteBuffer.wrap(largeBlockAfterMessage));
        assertThat(addressTruncate).isGreaterThan(0);

        // segment: 4
        new Random().nextBytes(largeBlock);
        address = fsLogStorage.append(ByteBuffer.wrap(largeBlock));
        assertThat(address).isGreaterThan(0);

        // segment: 5
        new Random().nextBytes(largeBlock);
        address = fsLogStorage.append(ByteBuffer.wrap(largeBlock));
        assertThat(address).isGreaterThan(0);

        assertThat(logDirectory.listFiles().length).isEqualTo(6);

        fsLogStorage.truncate(addressTruncate);

        assertThat(logDirectory.listFiles().length).isEqualTo(4);

        final long result = fsLogStorage.read(readBuffer, addressMessage);
        assertThat(result).isEqualTo(addressMessage + MSG.length);
        assertThat(readBuffer.array()).isEqualTo(MSG);

        assertThat(fsLogStorage.read(readBuffer, addressTruncate)).isEqualTo(LogStorage.OP_RESULT_NO_DATA);

        fsLogStorage.close();
    }

    protected byte[] readLogFile(final String logFilePath, final long address, final int capacity)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(capacity);

        final FileChannel fileChannel = FileUtil.openChannel(logFilePath, false);

        try
        {
            fileChannel.read(buffer, address);
        }
        catch (IOException e)
        {
            fail("fail to read from log file: " + logFilePath, e);
        }

        return buffer.array();
    }

}
