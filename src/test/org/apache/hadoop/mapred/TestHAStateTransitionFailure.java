/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ExitUtil.ExitException;
import org.junit.Test;

/**
 * Tests to verify the behavior of failing to fully start transition HA states.
 */
public class TestHAStateTransitionFailure {

  /**
   * Ensure that a failure to fully transition to the active state causes a
   * shutdown of the jobtracker.
   */
  @Test
  public void testFailureToTransitionCausesShutdown() throws Exception {
    MiniDFSCluster dfs = null;
    MiniMRHACluster cluster = null;
    try {
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster.Builder(conf)
          .numDataNodes(1)
          .format(true)
          .checkExitOnShutdown(false)
          .build();
      
      // Set the owner of the system directory to a different user to the one
      // that starts the JT. This will cause the JT to fail to transition to
      // the active state.
      FileSystem fs = dfs.getFileSystem();
      Path mapredSysDir = new Path(conf.get("mapred.system.dir"));
      fs.mkdirs(mapredSysDir);
      fs.setOwner(mapredSysDir, "mr", "mrgroup");

      cluster = new MiniMRHACluster(fs.getConf());
      try {
        cluster.getJobTrackerHaDaemon(0).makeActive();
        fail("Transitioned to active but should not have been able to.");
      } catch (ExitException ee) {
        assertExceptionContains("is not owned by", ee);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }
}
