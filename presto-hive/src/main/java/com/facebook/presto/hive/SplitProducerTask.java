/*
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
package com.facebook.presto.hive;

import com.facebook.presto.concurrent.SuspendableTask;
import com.facebook.presto.concurrent.TaskControl;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.hadoop.HadoopFileStatus.isFile;

public class SplitProducerTask
    implements SuspendableTask
{

    @Override
    public void run(TaskControl control)
    {
        //
//                Properties schema = getPartitionSchema(table, partition);
//                List<HivePartitionKey> partitionKeys = getPartitionKeys(table, partition);
//
//                Path path = new Path(getPartitionLocation(table, partition));
//                final Configuration configuration = hdfsEnvironment.getConfiguration(path);
//                final InputFormat<?, ?> inputFormat = getInputFormat(configuration, schema, false);
//
//                FileSystem fs = path.getFileSystem(configuration);
//
//                if (inputFormat instanceof SymlinkTextInputFormat) {
//                    JobConf jobConf = new JobConf(configuration);
//                    FileInputFormat.setInputPaths(jobConf, path);
//                    InputSplit[] splits = inputFormat.getSplits(jobConf, 0);
//                    for (InputSplit rawSplit : splits) {
//                        FileSplit split = ((SymlinkTextInputFormat.SymlinkTextInputSplit) rawSplit).getTargetSplit();
//
//                        // get the filesystem for the target path -- it may be a different hdfs instance
//                        FileSystem targetFilesystem = split.getPath().getFileSystem(configuration);
//                        FileStatus fileStatus = targetFilesystem.getFileStatus(split.getPath());
//                        queue.add(createHiveSplits(
//                                partitionName,
//                                fileStatus,
//                                targetFilesystem.getFileBlockLocations(fileStatus, split.getStart(), split.getLength()),
//                                split.getStart(),
//                                split.getLength(),
//                                schema,
//                                partitionKeys,
//                                false,
//                                session));
//                    }
//                    continue;
//                }
//
//                if (bucket.isPresent()) {
//                    Optional<FileStatus> bucketFile = getBucketFile(bucket.get(), fs, path);
//                    if (bucketFile.isPresent()) {
//                        FileStatus file = bucketFile.get();
//                        BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, file.getLen());
//                        boolean splittable = isSplittable(inputFormat, fs, file.getPath());
//
//                        hiveSplitSource.addToQueue(createHiveSplits(partitionName, file, blockLocations, 0, file.getLen(), schema, partitionKeys, splittable, session));
//                        continue;
//                    }
//                }
//
//                RemoteIterator<LocatedFileStatus> iterator = getLocatedFileStatusRemoteIterator(fs, path);
//
//                while (iterator.hasNext()) {
//                    LocatedFileStatus file = getLocatedFileStatus(iterator);
//                    boolean splittable = isSplittable(inputFormat, file.getPath().getFileSystem(configuration), file.getPath());
//
//                    hiveSplitSource.addToQueue(createHiveSplits(partitionName, file, file.getBlockLocations(), 0, file.getLen(), schema, partitionKeys, splittable, session));
//                }

    }


//    private RemoteIterator<LocatedFileStatus> getLocatedFileStatusRemoteIterator(FileSystem fileSystem, Path path)
//            throws IOException
//    {
//        try (TimeStat.BlockTimer timer = namenodeStats.getListLocatedStatus().time()) {
//            return directoryLister.list(fileSystem, path);
//        }
//        catch (IOException | RuntimeException e) {
//            namenodeStats.getListLocatedStatus().recordException(e);
//            throw e;
//        }
//    }


    //    private List<HiveSplit> createHiveSplits(
//            String partitionName,
//            FileStatus file,
//            BlockLocation[] blockLocations,
//            long start,
//            long length,
//            Properties schema,
//            List<HivePartitionKey> partitionKeys,
//            boolean splittable,
//            Session session)
//            throws IOException
//    {
//        ImmutableList.Builder<HiveSplit> builder = ImmutableList.builder();
//        if (splittable) {
//            for (BlockLocation blockLocation : blockLocations) {
//                // get the addresses for the block
//                List<HostAddress> addresses = toHostAddress(blockLocation.getHosts());
//
//                // divide the block into uniform chunks that are smaller than the max split size
//                int chunks = Math.max(1, (int) (blockLocation.getLength() / maxSplitSize.toBytes()));
//                // when block does not divide evenly into chunks, make the chunk size slightly bigger than necessary
//                long targetChunkSize = (long) Math.ceil(blockLocation.getLength() * 1.0 / chunks);
//
//                long chunkOffset = 0;
//                while (chunkOffset < blockLocation.getLength()) {
//                    // adjust the actual chunk size to account for the overrun when chunks are slightly bigger than necessary (see above)
//                    long chunkLength = Math.min(targetChunkSize, blockLocation.getLength() - chunkOffset);
//
//                    builder.add(new HiveSplit(connectorId,
//                            table.getDbName(),
//                            table.getTableName(),
//                            partitionName,
//                            file.getPath().toString(),
//                            blockLocation.getOffset() + chunkOffset,
//                            chunkLength,
//                            schema,
//                            partitionKeys,
//                            addresses,
//                            session));
//
//                    chunkOffset += chunkLength;
//                }
//                checkState(chunkOffset == blockLocation.getLength(), "Error splitting blocks");
//            }
//        }
//        else {
//            // not splittable, use the hosts from the first block if it exists
//            List<HostAddress> addresses = ImmutableList.of();
//            if (blockLocations.length > 0) {
//                addresses = toHostAddress(blockLocations[0].getHosts());
//            }
//
//            builder.add(new HiveSplit(connectorId,
//                    table.getDbName(),
//                    table.getTableName(),
//                    partitionName,
//                    file.getPath().toString(),
//                    start,
//                    length,
//                    schema,
//                    partitionKeys,
//                    addresses,
//                    session));
//        }
//        return builder.build();
//    }

//    private LocatedFileStatus getLocatedFileStatus(RemoteIterator<LocatedFileStatus> iterator)
//            throws IOException
//    {
//        try (TimeStat.BlockTimer timer = namenodeStats.getRemoteIteratorNext().time()) {
//            return iterator.next();
//        }
//        catch (IOException | RuntimeException e) {
//            namenodeStats.getRemoteIteratorNext().recordException(e);
//            throw e;
//        }
//    }

    private static Optional<FileStatus> getBucketFile(HiveBucketing.HiveBucket bucket, FileSystem fs, Path path)
    {
        FileStatus[] statuses = listStatus(fs, path);

        if (statuses.length != bucket.getBucketCount()) {
            return Optional.absent();
        }

        Map<String, FileStatus> map = new HashMap<>();
        List<String> paths = new ArrayList<>();
        for (FileStatus status : statuses) {
            if (!isFile(status)) {
                return Optional.absent();
            }
            String pathString = status.getPath().toString();
            map.put(pathString, status);
            paths.add(pathString);
        }

        // Hive sorts the paths as strings lexicographically
        Collections.sort(paths);

        String pathString = paths.get(bucket.getBucketNumber());
        return Optional.of(map.get(pathString));
    }

    private static FileStatus[] listStatus(FileSystem fs, Path path)
    {
        try {
            return fs.listStatus(path);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

}
