/*
 * Copyright 2013-2014 eXascale Infolab, University of Fribourg. All rights reserved.
 */
package org.apache.hadoop.hadaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
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

  private final List<ParameterGeneration> parameterGenerations;
  private final List<ParameterFile> parameterFiles;
  private final Configuration configuration;

  private final List<BalancerNode> dataNodes = new ArrayList<BalancerNode>();

  private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
      CONCURRENT_TASKS, CONCURRENT_TASKS, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
  private final CompletionService<BalancerResult> completionService =
      new ExecutorCompletionService<BalancerResult>(threadPool);

  Balancer(List<ParameterGeneration> parameterGenerations, List<ParameterFile> parameterFiles, Configuration configuration) {
    if (parameterGenerations == null) throw new IllegalArgumentException();
    if (parameterFiles == null) throw new IllegalArgumentException();
    if (configuration == null) throw new IllegalArgumentException();

    this.parameterGenerations = parameterGenerations;
    this.parameterFiles = parameterFiles;
    this.configuration = configuration;
  }

  void run() throws IOException, InterruptedException {
    // Get the distributed filesystem
    FileSystem fileSystem = FileSystem.get(configuration);
    if (!(fileSystem instanceof DistributedFileSystem)) {
      throw new IllegalStateException("Filesystem " + fileSystem.getUri() + " is not an HDFS filesystem");
    }
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;

    // Populate data nodes
    DatanodeInfo[] dataNodeInfos = dfs.getDataNodeStats(HdfsConstants.DatanodeReportType.LIVE);
    for (DatanodeInfo dataNode : dataNodeInfos) {
      if (dataNode.isDecommissioned() || dataNode.isDecommissionInProgress()) {
        continue;
      }

      dataNodes.add(new BalancerNode(dataNode));
    }

    // Populate balancer files
    List<BalancerFile> balancerFiles = getBalancerFiles(dfs);

    // Create our policy
    IBlockPlacementPolicy policy = new HadapsBlockPlacementPolicy(dataNodes, parameterGenerations);
    policy.initialize(configuration);

    // Now balance each file
    for (BalancerFile balancerFile : balancerFiles) {
      if (threadPool.getActiveCount() >= CONCURRENT_TASKS) {
        // Await completion of any submitted task

        try {
          BalancerResult result = completionService.take().get();
        } catch (ExecutionException e) {
          LOG.warn(e.getLocalizedMessage(), e);
        }
      }

      completionService.submit(new BalancerTask(balancerFile, policy, dfs, dataNodes));
    }

    // Await completion of any submitted task
    while (threadPool.getActiveCount() > 0) {
      try {
        BalancerResult result = completionService.take().get();
      } catch (ExecutionException e) {
        LOG.warn(e.getLocalizedMessage(), e);
      }
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

  private void populateBalancerFiles(
      List<BalancerFile> balancerFiles, FileStatus status, ParameterFile parameterFile, DistributedFileSystem fileSystem)
      throws IOException {
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
