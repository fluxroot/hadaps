/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class HadapsTest extends Configured implements Tool {

  private static final String USAGE = String.format("Usage: java %s%n"
          + "  [-in <input directory>]%n"
          + "  [-out <output directory>]%n",
      HadapsTest.class.getSimpleName()
  );

  public static class HadapsTestMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
    }
  }

  public static class HadapsTestReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
    }
  }

  private static class Parameters {
    private static final Parameters DEFAULT = new Parameters(
        "hadaps.testdir", "hadaps.resultdir"
    );

    private final String inputDirectory;
    private final String outputDirectory;

    private Parameters(String inputDirectory, String outputDirectory) {
      assert inputDirectory != null;
      assert outputDirectory != null;

      this.inputDirectory = inputDirectory;
      this.outputDirectory = outputDirectory;
    }
  }

  private int runJob(List<Path> files, Parameters parameters, Configuration configuration)
      throws IOException, ClassNotFoundException, InterruptedException {
    assert files != null;
    assert parameters != null;
    assert configuration != null;

    // Create job
    Job job = Job.getInstance(configuration);
    job.setJarByClass(HadapsTest.class);
    job.setJobName(HadapsTest.class.getSimpleName());

    // Add input files
    for (Path file : files) {
      FileInputFormat.addInputPath(job, file);
    }

    // Add output directory
    FileOutputFormat.setOutputPath(job, new Path(parameters.outputDirectory));

    job.setMapperClass(HadapsTestMapper.class);
    job.setReducerClass(HadapsTestReducer.class);

    boolean result = job.waitForCompletion(true);
    return result ? 0 : -1;
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

  private List<Path> getFiles(Parameters parameters, Configuration configuration) throws IOException {
    assert parameters != null;
    assert configuration != null;

    // Switch to directory
    FileContext fileContext = FileContext.getFileContext(configuration);
    Path directory = fileContext.makeQualified(new Path(parameters.inputDirectory));
    if (!fileContext.util().exists(directory)) {
      throw new FileNotFoundException("Directory does not exist: " + directory.toString());
    } else if (!fileContext.getFileStatus(directory).isDirectory()) {
      throw new FileNotFoundException("Is not a directory: " + directory.toString());
    }
    fileContext.setWorkingDirectory(directory);

    // Get list of all files
    List<Path> files = new ArrayList<Path>();
    populateFiles(files, fileContext, directory);

    return files;
  }

  private Parameters getParameters(String[] args) {
    assert args != null;

    // Default parameters
    String inputDirectory = Parameters.DEFAULT.inputDirectory;
    String outputDirectory = Parameters.DEFAULT.outputDirectory;

    // Parse arguments
    Iterator<String> tokens = Arrays.asList(args).iterator();
    while (tokens.hasNext()) {
      String token = tokens.next();

      if (token.equalsIgnoreCase("-in") && tokens.hasNext()) {
        inputDirectory = tokens.next().trim();
      } else if (token.equalsIgnoreCase("-out") && tokens.hasNext()) {
        outputDirectory = tokens.next().trim();
      }
    }

    return new Parameters(inputDirectory, outputDirectory);
  }

  @Override
  public int run(String[] args) throws Exception {
    // Get parameters
    Parameters parameters = getParameters(args);

    // Get configuration
    Configuration configuration = getConf();

    // Get list of all files
    // We need to build our file list manually as FileInputFormat.addInputPath()
    // does not recurse deeply into a directory
    List<Path> files = getFiles(parameters, configuration);

    // Run the job
    return runJob(files, parameters, configuration);
  }

  public static void main(String[] args) {
    if (args.length == 1 && args[0].equalsIgnoreCase("-help")) {
      System.out.print(USAGE);
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new Configuration(), new HadapsTest(), args));
    } catch (Throwable e) {
      System.err.format("Error: %s%n", e.toString());
      System.exit(-1);
    }
  }

}
