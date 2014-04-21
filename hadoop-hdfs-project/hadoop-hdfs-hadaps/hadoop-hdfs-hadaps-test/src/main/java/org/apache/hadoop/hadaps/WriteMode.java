/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

class WriteMode {

  private static final Logger LOG = LoggerFactory.getLogger(WriteMode.class);

  private static final int ONE_MEGABYTE = 1024 * 1024;

  private final Parameters parameters;
  private final Configuration configuration;
  private final FileContext fileContext;

  WriteMode(Parameters parameters, Configuration configuration) throws UnsupportedFileSystemException {
    if (parameters == null) throw new IllegalArgumentException();
    if (configuration == null) throw new IllegalArgumentException();

    this.parameters = parameters;
    this.configuration = configuration;
    fileContext = FileContext.getFileContext(configuration);
  }

  void run() throws IOException, InterruptedException, NoSuchAlgorithmException {
    // Write files
    List<Path> files = write();

    // Wait for replication
    waitForReplication(files);
  }

  private List<Path> write() throws IOException, NoSuchAlgorithmException {
    List<Path> files = new ArrayList<Path>();

    MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
    Random random = new Random();

    // Create test directory
    Path outputDirectory = fileContext.makeQualified(new Path(parameters.outputDirectory));
    if (!fileContext.util().exists(outputDirectory)) {
      LOG.info("Creating non-existent directory {}", outputDirectory);
      fileContext.mkdir(outputDirectory, FsPermission.getDirDefault(), true);
    }
    fileContext.setWorkingDirectory(outputDirectory);
    LOG.debug("Working directory is now {}", fileContext.getWorkingDirectory().toString());

    // Create files
    for (int j = 0; j < parameters.count; ++j) {
      Path file = null;
      FSDataOutputStream outputStream = null;
      String digest = null;

      int size = (parameters.minsize + random.nextInt(parameters.maxsize - parameters.minsize + 1))
          * ONE_MEGABYTE;

      try {
        // Create file
        file = new Path(Long.toString(System.currentTimeMillis()));
        long blockSize = file.getFileSystem(configuration).getDefaultBlockSize(file);
        outputStream = fileContext.create(
            file, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
            Options.CreateOpts.createParent(), Options.CreateOpts.blockSize(blockSize));

        // Write random bytes
        int currentSize = size;
        byte[] bytes = new byte[ONE_MEGABYTE]; // 1 megabyte
        while (currentSize > 0) {
          int length = Math.min(currentSize, ONE_MEGABYTE);
          random.nextBytes(bytes);
          messageDigest.update(bytes, 0, length);
          outputStream.write(bytes, 0, length);
          currentSize -= length;
        }

        // Get the digest string
        digest = Utils.getHexString(messageDigest.digest());
      } finally {
        if (outputStream != null) {
          outputStream.close();
        }
      }

      // Rename to digest string
      Path digestFile = fileContext.makeQualified(new Path(digest));
      fileContext.rename(file, digestFile, Options.Rename.OVERWRITE);
      files.add(digestFile);

      // Create control file
      Path controlFile = digestFile.suffix(".control");
      SequenceFile.Writer writer = SequenceFile.createWriter(configuration,
          SequenceFile.Writer.file(controlFile),
          SequenceFile.Writer.keyClass(Text.class),
          SequenceFile.Writer.valueClass(LongWritable.class),
          SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
      writer.append(new Text(controlFile.toString()), new LongWritable(size));
      writer.close();

      LOG.info("Created file {}", digestFile.toString());
    }

    return files;
  }

  private void waitForReplication(List<Path> files) throws IOException, InterruptedException {
    assert files != null;

    for (Path file : files) {
      FileStatus status = fileContext.getFileStatus(file);
      short replication = status.getReplication();
      LOG.info("Waiting for replication to adjust to factor {} for file {}",
          replication, file.toString());

      boolean done = false;
      while (!done) {
        done = true;

        // For each block location, check number of hosts
        BlockLocation[] locations = fileContext.getFileBlockLocations(file, 0, status.getLen());
        for (BlockLocation location : locations) {
          if (location.getHosts().length != replication) {
            done = false;
            break;
          }
        }

        if (!done) {
          Thread.sleep(1000);
        }
      }

      LOG.info("Replicated file {}", file.toString());
    }
  }

}
