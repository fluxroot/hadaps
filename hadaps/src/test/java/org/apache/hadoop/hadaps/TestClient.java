/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Random;

class TestClient {

  private static final Logger LOG = LoggerFactory.getLogger(TestClient.class);

  private static final String USAGE = "Usage: java "
      + TestClient.class.getSimpleName()
      + " [read|write]"
      + " [-name <filename>]";

  private static enum Mode {
    READ, WRITE
  }

  private static class Parameters {
    private static final Parameters DEFAULT = new Parameters(
        Mode.READ, "hadaps.test"
    );

    private final Mode mode;
    private final String filename;

    private Parameters(Mode mode, String filename) {
      this.mode = mode;
      this.filename = filename;
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
    Path file = new Path(parameters.filename);
    FileContext fileContext = FileContext.getFileContext(configuration);
    FSDataInputStream inputStream = null;
    try {
      inputStream = fileContext.open(file);

      byte[] bytes = new byte[1024 * 1024]; // 1 megabyte
      int length = inputStream.read(bytes);
      while (length != -1) {
        messageDigest.update(bytes, 0, length);
        length = inputStream.read(bytes);
      }

      LOG.info("SHA-1: " + new HexBinaryAdapter().marshal(messageDigest.digest()));
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
  }

  /**
   * Creates a file in HDFS and write random bytes.
   */
  private void write(Parameters parameters, Configuration configuration)
      throws IOException, NoSuchAlgorithmException {
    MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
    Random random = new Random();
    Path file = new Path(parameters.filename);
    FileContext fileContext = FileContext.getFileContext(configuration);

    FSDataOutputStream outputStream = null;
    try {
      outputStream = fileContext.create(
          file, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
          Options.CreateOpts.createParent());

      byte[] bytes = new byte[1024 * 1024]; // 1 megabyte
      for (int i = 0; i < 100; ++i) {
        random.nextBytes(bytes);
        messageDigest.update(bytes);
        outputStream.write(bytes);
      }

      LOG.info("SHA-1: " + new HexBinaryAdapter().marshal(messageDigest.digest()));
    } finally {
      if (outputStream != null) {
        outputStream.close();
      }
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
      String filename = Parameters.DEFAULT.filename;

      // Get arguments
      Iterator<String> tokens = Arrays.asList(args).iterator();
      while (tokens.hasNext()) {
        String token = tokens.next();

        if (token.equalsIgnoreCase("read")) {
          mode = Mode.READ;
        } else if (token.equalsIgnoreCase("write")) {
          mode = Mode.WRITE;
        } else if (token.equalsIgnoreCase("-name") && tokens.hasNext()) {
          filename = tokens.next().trim();
        }
      }
      LOG.info("Using mode: " + mode);
      LOG.info("Using filename: " + filename);

      return new TestClient().run(new Parameters(mode, filename), getConf());
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
      System.exit(-1);
    }
  }

}
