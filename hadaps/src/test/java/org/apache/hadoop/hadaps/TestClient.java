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
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

class TestClient {

  private static final Logger LOG = LoggerFactory.getLogger(TestClient.class);

  private static final String USAGE = String.format("Usage: java %s <command>%n"
      + "%n"
      + "Available commands are:%n"
      + "  read [-dir <test directory>]%n"
      + "       [-iteration <number of iterations>]%n"
      + "       [-csv <filename>]%n"
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
        Mode.READ, "hadaps.testdir", "hadaps.csv", 1, 50, 10, 10
    );

    private final Mode mode;
    private final String directory;
    private final String csv;
    private final int iteration;
    private final int count;
    private final int minsize;
    private final int maxsize;

    private Parameters(Mode mode, String directory, String csv, int iteration, int count, int minsize, int maxsize) {
      this.mode = mode;
      this.directory = directory;
      this.csv = csv;
      this.iteration = iteration;
      this.count = count;
      this.minsize = minsize;
      this.maxsize = maxsize;
    }
  }

  private TestClient() {
  }

  private List<Path> getFiles(FileContext fileContext, Path path) throws IOException {
    assert fileContext != null;
    assert path != null;

    List<Path> files = new ArrayList<Path>();

    populateFiles(files, fileContext, path);

    return files;
  }

  private void populateFiles(List<Path> files, FileContext fileContext, Path path) throws IOException {
    assert files != null;
    assert fileContext != null;
    assert path != null;

    FileStatus status = fileContext.getFileStatus(path);
    if (status.isFile()) {
      files.add(path);
    } else if (status.isDirectory()) {
      RemoteIterator<FileStatus> stats = fileContext.listStatus(path);
      while (stats.hasNext()) {
        FileStatus stat = stats.next();
        populateFiles(files, fileContext, stat.getPath());
      }
    }
  }

  /**
   * Reads files from HDFS.
   */
  private void read(Parameters parameters, Configuration configuration)
      throws IOException, NoSuchAlgorithmException {
    assert parameters != null;
    assert configuration != null;

    MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
    Random random = new Random();

    // Switch to test directory
    FileContext fileContext = FileContext.getFileContext(configuration);
    Path directory = fileContext.makeQualified(new Path(parameters.directory));
    if (!fileContext.util().exists(directory)) {
      throw new FileNotFoundException("Directory does not exist: " + directory.toString());
    } else if (!fileContext.getFileStatus(directory).isDirectory()) {
      throw new FileNotFoundException("Is not a directory: " + directory.toString());
    }
    fileContext.setWorkingDirectory(directory);
    LOG.debug("Working directory is now: {}", fileContext.getWorkingDirectory().toString());

    // Get list of all files
    List<Path> files = getFiles(fileContext, directory);
    LOG.debug("Using files: {}", files.toString());

    List<Statistic> statistics = new ArrayList<Statistic>();

    Csv csv = null;
    try {
      // Create CSV file
      csv = new Csv(parameters.csv);

      for (int i = 1; i < parameters.iteration + 1; ++i) {
        LOG.info("Starting iteration {}", i);
        System.out.format("Starting iteration %d%n", i);

        // Read all files in random order
        List<Path> filesPool = new ArrayList<Path>(files);

        while (filesPool.size() > 0) {
          // Get the file
          int index = random.nextInt(filesPool.size());
          Path file = fileContext.makeQualified(filesPool.get(index));
          FileStatus status = fileContext.getFileStatus(file);

          // Read the file
          FSDataInputStream inputStream = null;
          try {
            inputStream = fileContext.open(file);

            LOG.info("Reading file {}", file.toString());
            System.out.format("Reading file %s%n", file.toString());

            long startTime = Time.now();

            byte[] bytes = new byte[ONE_MEGABYTE]; // 1 megabyte
            int length = inputStream.read(bytes);
            while (length != -1) {
              messageDigest.update(bytes, 0, length);
              length = inputStream.read(bytes);
            }

            long duration = Time.now() - startTime;

            // Compare the digest
            String digest = Utils.getHexString(messageDigest.digest());
            if (file.getName().equalsIgnoreCase(digest)) {
              LOG.info("Iteration {}: Read file {} in {}", i, file.toString(), Utils.getPrettyTime(duration));
              System.out.format("Iteration %d: Read file %s in %s%n", i, file.toString(), Utils.getPrettyTime(duration));

              statistics.add(new Statistic(i, file.toString(), status.getReplication(), status.getLen(), duration));
            } else {
              LOG.warn("Content does not match filename: {} != {}", digest, file.toString());
              System.out.format("Content does not match filename: %s != %s%n", digest, file.toString());
            }

            filesPool.remove(index);
          } finally {
            if (inputStream != null) {
              inputStream.close();
            }
          }
        }
      }

      // Write statistics to CSV file
      csv.write(statistics);
    } finally {
      if (csv != null) {
        csv.close();
      }
    }
  }

  /**
   * Creates files and write random bytes.
   */
  private void write(Parameters parameters, Configuration configuration)
      throws IOException, NoSuchAlgorithmException {
    assert parameters != null;
    assert configuration != null;

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
    assert parameters != null;
    assert configuration != null;

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
      String csv = Parameters.DEFAULT.csv;
      int iteration = Parameters.DEFAULT.iteration;
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
        } else if (token.equalsIgnoreCase("-csv") && tokens.hasNext()) {
          csv = tokens.next().trim();
        } else if (token.equalsIgnoreCase("-iteration") && tokens.hasNext()) {
          iteration = Integer.parseInt(tokens.next().trim());

          if (iteration <= 0) {
            throw new IllegalStateException("Invalid iteration format");
          }
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
      LOG.info("Using iteration: " + iteration);
      LOG.info("Using count: " + count);
      LOG.info("Using minsize: " + minsize);
      LOG.info("Using maxsize: " + maxsize);

      return new TestClient().run(new Parameters(mode, directory, csv, iteration, count, minsize, maxsize), getConf());
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
