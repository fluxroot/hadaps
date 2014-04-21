/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ReadMode {

  private static final Logger LOG = LoggerFactory.getLogger(ReadMode.class);

  private static final int ONE_MEGABYTE = 1024 * 1024;

  private final Parameters parameters;
  private final Configuration configuration;
  private final FileContext fileContext;

  public static class ReadModeMapper extends Mapper<Text, LongWritable, Text, Text> {
    private FileContext fileContext;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      fileContext = FileContext.getFileContext(context.getConfiguration());
    }

    @Override
    protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
      String filename = key.toString();
      filename = filename.substring(0, filename.lastIndexOf('.'));
      Path file = new Path(filename);
      long size = value.get();
      short replication = fileContext.getFileStatus(file).getReplication();

      // Open file
      InputStream inputStream = null;
      try {
        inputStream = fileContext.open(file);

        long currentSize = 0;
        byte[] bytes = new byte[ONE_MEGABYTE]; // 1 megabyte

        // Read file
        long startTime = System.currentTimeMillis();
        while (currentSize < size) {
          int length = inputStream.read(bytes, 0, bytes.length);
          if (length == -1) {
            break;
          }
          currentSize += length;
        }
        long duration = System.currentTimeMillis() - startTime;

        // Write statistic
        context.write(new Text(filename), new Text(replication + " " + currentSize + " " + duration));
      } finally {
        if (inputStream != null) {
          inputStream.close();
        }
      }
    }
  }

  public static class ReadModeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text value : values) {
        context.write(key, value);
      }
    }
  }

  public ReadMode(Parameters parameters, Configuration configuration) throws UnsupportedFileSystemException {
    if (parameters == null) throw new IllegalArgumentException();
    if (configuration == null) throw new IllegalArgumentException();

    this.parameters = parameters;
    this.configuration = configuration;
    fileContext = FileContext.getFileContext(configuration);
  }

  void run() throws IOException, InterruptedException, ClassNotFoundException {
    // Get outputDirectory
    Path outputDirectory = fileContext.makeQualified(new Path(parameters.outputDirectory));

    // Get list of all control files
    List<Path> files = getFiles();
    LOG.debug("Using files: {}", files.toString());

    Csv csv = null;
    try {
      // Create CSV file
      csv = new Csv(parameters.csv);
      LOG.info("Created csv file {}", Paths.get(parameters.csv).toAbsolutePath().toString());

      // Run the test
      List<Statistic> statistics = read(files, outputDirectory);

      // Write statistics to CSV file
      LOG.info("Writing statistics to csv file");
      csv.write(statistics);
    } finally {
      if (csv != null) {
        csv.close();
      }
    }
  }

  private List<Path> getFiles() throws IOException {
    // Switch to directory
    Path inputDirectory = fileContext.makeQualified(new Path(parameters.inputDirectory));
    if (!fileContext.util().exists(inputDirectory)) {
      throw new FileNotFoundException("Directory does not exist: " + inputDirectory.toString());
    } else if (!fileContext.getFileStatus(inputDirectory).isDirectory()) {
      throw new FileNotFoundException("Is not a directory: " + inputDirectory.toString());
    }
    fileContext.setWorkingDirectory(inputDirectory);
    LOG.debug("Working directory is now {}", fileContext.getWorkingDirectory().toString());

    // Get list of all files
    List<Path> files = new ArrayList<Path>();
    populateFiles(files, fileContext, inputDirectory);

    return files;
  }

  private void populateFiles(List<Path> files, FileContext fileContext, Path path) throws IOException {
    assert files != null;
    assert fileContext != null;
    assert path != null;

    FileStatus status = fileContext.getFileStatus(path);
    if (status.isFile()) {
      if (path.getName().endsWith(".control")) {
        files.add(path);
      }
    } else if (status.isDirectory()) {
      RemoteIterator<FileStatus> stats = fileContext.listStatus(path);
      while (stats.hasNext()) {
        FileStatus stat = stats.next();
        populateFiles(files, fileContext, stat.getPath());
      }
    }
  }

  private List<Statistic> read(List<Path> files, Path outputDirectory)
      throws IOException, ClassNotFoundException, InterruptedException {
    assert files != null;
    assert outputDirectory != null;

    List<Statistic> statistics = new ArrayList<Statistic>();

    for (int i = 1; i < parameters.iteration + 1; ++i) {
      LOG.info("Starting iteration {}", i);

      LOG.info("Deleting output directory {}", outputDirectory);
      fileContext.delete(outputDirectory, true);

      // Create job
      Job job = Job.getInstance(configuration);
      job.setJarByClass(HadapsTest.class);
      job.setJobName(HadapsTest.class.getSimpleName());

      // Add input files
      for (Path file : files) {
        FileInputFormat.addInputPath(job, file);
      }
      job.setInputFormatClass(SequenceFileInputFormat.class);

      // Add mapper
      job.setMapperClass(ReadModeMapper.class);
      job.setReducerClass(ReadModeReducer.class);

      // Add output directory
      FileOutputFormat.setOutputPath(job, outputDirectory);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setNumReduceTasks(1);

      // Start the job
      long startTime = Time.now();
      boolean result = job.waitForCompletion(true);
      long totalDuration = Time.now() - startTime;

      // Collect statistic
      if (result) {
        statistics.addAll(analyze(i, totalDuration, outputDirectory));
      } else {
        LOG.warn("Job failed for iteration {}!", i);
      }
    }

    return statistics;
  }

  private List<Statistic> analyze(int i, long totalDuration, Path outputDirectory) throws IOException {
    assert outputDirectory != null;

    List<Statistic> statistics = new ArrayList<Statistic>();

    Path resultFile = new Path(outputDirectory, "part-r-00000");
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(fileContext.open(resultFile)));
      String line;
      while ((line = reader.readLine()) != null) {
        String[] tokens = line.split("\\s");
        if (tokens.length != 4) {
          throw new IllegalStateException("Invalid number of tokens");
        }

        String filename = tokens[0];
        short replication = Short.parseShort(tokens[1]);
        long size = Long.parseLong(tokens[2]);
        long duration = Long.parseLong(tokens[3]);

        statistics.add(new Statistic(i, filename, replication, size, duration));
      }

      statistics.add(new Statistic(i, "TOTAL", (short) 0, 0, totalDuration));

      return statistics;
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

}
