/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

class Balancer {

  private static final Logger LOG = LoggerFactory.getLogger(Balancer.class);

  private static final int CONCURRENT_TASKS = 3;

  private final List<ParameterFile> parameterFiles;
  private final Configuration configuration;

  private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
      CONCURRENT_TASKS, CONCURRENT_TASKS, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
  private final CompletionService<Integer> completionService =
      new ExecutorCompletionService<Integer>(threadPool);

  Balancer(List<ParameterFile> parameterFiles, Configuration configuration) {
    if (parameterFiles == null) throw new IllegalArgumentException();
    if (configuration == null) throw new IllegalArgumentException();

    this.parameterFiles = parameterFiles;
    this.configuration = configuration;
  }

  void run() throws IOException, InterruptedException {
    // Get the distributed filesystem
    FileSystem fs = FileSystem.get(configuration);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalStateException("Filesystem " + fs.getUri() + " is not an HDFS filesystem");
    }
    DistributedFileSystem fileSystem = (DistributedFileSystem) fs;

    // Create BalancerNameNode
    BalancerNameNode nameNode = new BalancerNameNode(fileSystem);

    // Create our policy
    HadapsBlockPlacementPolicy policy = new HadapsBlockPlacementPolicy(configuration, null,
        NetworkTopology.getInstance(configuration));

    // Populate balancer files
    List<BalancerFile> files = getBalancerFiles(fileSystem);

    // Now balance each file
    for (BalancerFile file : files) {
      while (threadPool.getActiveCount() >= CONCURRENT_TASKS) {
        // Await completion of any submitted task
        completionService.poll(1, TimeUnit.SECONDS);
      }

      completionService.submit(new BalancerTask(file, policy, nameNode));
    }

    // Await completion of any submitted task
    while (threadPool.getActiveCount() > 0) {
      completionService.poll(1, TimeUnit.SECONDS);
    }

    // Initiate a proper shutdown
    threadPool.shutdown();
    threadPool.awaitTermination(10, TimeUnit.SECONDS);
  }

  private List<BalancerFile> getBalancerFiles(DistributedFileSystem fileSystem) throws IOException {
    List<BalancerFile> balancerFiles = new ArrayList<BalancerFile>();

    // Iterate over each pattern
    for (ParameterFile parameterFile : parameterFiles) {
      Path globPath = new Path(parameterFile.getName());

      FileStatus[] stats = fileSystem.globStatus(globPath);
      if (stats != null && stats.length > 0) {
        // We have some matching paths

        List<BalancerFile> matchingFiles = new ArrayList<BalancerFile>();

        for (FileStatus stat : stats) {
          populateBalancerFiles(matchingFiles, stat, parameterFile, fileSystem);
        }

        balancerFiles.addAll(matchingFiles);

        LOG.info("Matching files for pattern \"{}\": {}", globPath.toString(), matchingFiles);
      } else {
        LOG.info("No matching files for pattern \"{}\"", globPath.toString());
      }
    }

    Collections.sort(balancerFiles);

    return balancerFiles;
  }

  private void populateBalancerFiles(List<BalancerFile> balancerFiles, FileStatus status,
      ParameterFile parameterFile, DistributedFileSystem fileSystem) throws IOException {
    assert balancerFiles != null;
    assert status != null;
    assert parameterFile != null;
    assert fileSystem != null;

    if (status.isFile()) {
      balancerFiles.add(new BalancerFile(status, parameterFile, fileSystem));
    } else if (status.isDirectory()) {
      // Recurse into directory

      FileStatus[] stats = fileSystem.listStatus(status.getPath());
      for (FileStatus stat : stats) {
        populateBalancerFiles(balancerFiles, stat, parameterFile, fileSystem);
      }
    }
  }

}
