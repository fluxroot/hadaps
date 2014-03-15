/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Random;

class TestClient {

  private static final Logger LOG = LoggerFactory.getLogger(TestClient.class);

  private static final String USAGE = String.format("Usage: java %s <command>%n"
      + "%n"
      + "Available commands are:%n"
      + "  read [-dir <test directory>]%n"
      + "    Reads files randomly from the test directory.%n"
      + "%n"
      + "  write [-dir <test directory>]%n"
      + "        [-count <number of files>]%n"
      + "        [-size <minsize in megabytes>:<maxsize in megabytes>]%n"
      + "    Writes files with random content to the test directory.%n",
      TestClient.class.getSimpleName());

  private static final int ONE_MEGABYTE = 1024 * 1024;

  private static enum Mode {
    READ, WRITE
  }

  private static class Parameters {
    private static final Parameters DEFAULT = new Parameters(
        Mode.READ, "hadaps.testdir", 50, 10, 10
    );

    private final Mode mode;
    private final String directory;
    private final int count;
    private final int minsize;
    private final int maxsize;

    private Parameters(Mode mode, String directory, int count, int minsize, int maxsize) {
      this.mode = mode;
      this.directory = directory;
      this.count = count;
      this.minsize = minsize;
      this.maxsize = maxsize;
    }
  }

  private TestClient() {
  }

  /**
   * Reads a file from HDFS.
   */
  private void read(Parameters parameters, Configuration configuration)
      throws IOException, NoSuchAlgorithmException {
    MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
    Path file = new Path(parameters.directory);
    FileContext fileContext = FileContext.getFileContext(configuration);
    FSDataInputStream inputStream = null;
    try {
      inputStream = fileContext.open(file);

      byte[] bytes = new byte[ONE_MEGABYTE]; // 1 megabyte
      int length = inputStream.read(bytes);
      while (length != -1) {
        messageDigest.update(bytes, 0, length);
        length = inputStream.read(bytes);
      }

      LOG.info("SHA-1: " + Utils.getHexString(messageDigest.digest()));
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
  }

  /**
   * Creates files and write random bytes.
   */
  private void write(Parameters parameters, Configuration configuration)
      throws IOException, NoSuchAlgorithmException {
    MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
    Random random = new Random();

    // Create test directory
    FileContext fileContext = FileContext.getFileContext(configuration);
    Path directory = fileContext.makeQualified(new Path(parameters.directory));
    if (!fileContext.util().exists(directory)) {
      LOG.info("Creating non-existent directory {}", directory);
      System.out.format("Creating non-existent directory %s%n", directory);
      fileContext.mkdir(directory, FsPermission.getDirDefault(), true);
    }
    fileContext.setWorkingDirectory(directory);
    LOG.debug("Working directory is now: {}", fileContext.getWorkingDirectory().toString());

    // Create files
    for (int j = 0; j < parameters.count; ++j) {
      Path file = null;
      FSDataOutputStream outputStream = null;
      String digest = null;
      try {
        // Create file
        file = new Path(Long.toString(System.currentTimeMillis()));
        outputStream = fileContext.create(
            file, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
            Options.CreateOpts.createParent());

        // Write random bytes
        int size = (parameters.minsize + random.nextInt(parameters.maxsize - parameters.minsize + 1))
            * ONE_MEGABYTE;
        byte[] bytes = new byte[ONE_MEGABYTE]; // 1 megabyte
        while (size > 0) {
          int length = Math.min(size, ONE_MEGABYTE);
          random.nextBytes(bytes);
          messageDigest.update(bytes, 0, length);
          outputStream.write(bytes, 0, length);
          size -= length;
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

      LOG.info("Created file {}", digestFile.toString());
      System.out.format("Created file %s%n", digestFile.toString());
    }
  }

  private int run(Parameters parameters, Configuration configuration)
      throws IOException, NoSuchAlgorithmException {
    switch (parameters.mode) {
      case READ:
        read(parameters, configuration);
        break;
      case WRITE:
        write(parameters, configuration);
        break;
      default:
        throw new IllegalArgumentException();
    }

    return 0;
  }

  private static class Cli extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
      // Default parameters
      Mode mode = Parameters.DEFAULT.mode;
      String directory = Parameters.DEFAULT.directory;
      int count = Parameters.DEFAULT.count;
      int minsize = Parameters.DEFAULT.minsize;
      int maxsize = Parameters.DEFAULT.maxsize;

      // Get arguments
      Iterator<String> tokens = Arrays.asList(args).iterator();
      while (tokens.hasNext()) {
        String token = tokens.next();

        if (token.equalsIgnoreCase("read")) {
          mode = Mode.READ;
        } else if (token.equalsIgnoreCase("write")) {
          mode = Mode.WRITE;
        } else if (token.equalsIgnoreCase("-dir") && tokens.hasNext()) {
          directory = tokens.next().trim();
        } else if (token.equalsIgnoreCase("-count") && tokens.hasNext()) {
          count = Integer.parseInt(tokens.next().trim());

          if (count <= 0) {
            throw new IllegalStateException("Invalid count format");
          }
        } else if (token.equalsIgnoreCase("-size") && tokens.hasNext()) {
          String[] sizeTokens = tokens.next().trim().split(":");
          if (sizeTokens.length == 2) {
            minsize = Integer.parseInt(sizeTokens[0].trim());
            maxsize = Integer.parseInt(sizeTokens[1].trim());

            if (minsize <= 0 || maxsize <= 0) {
              throw new IllegalStateException("Invalid size format");
            }
          } else {
            throw new IllegalStateException("Invalid size format");
          }
        }
      }
      LOG.info("Using mode: " + mode);
      LOG.info("Using directory: " + directory);
      LOG.info("Using count: " + count);
      LOG.info("Using minsize: " + minsize);
      LOG.info("Using maxsize: " + maxsize);

      return new TestClient().run(new Parameters(mode, directory, count, minsize, maxsize), getConf());
    }
  }

  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting " + TestClient.class.getSimpleName() + " due to an exception", e);
      System.err.format("Error: %s%n", e.toString());
      System.exit(-1);
    }
  }

}
