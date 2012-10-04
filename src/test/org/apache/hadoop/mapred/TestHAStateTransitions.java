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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.ha.FailoverController;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer;
import org.apache.hadoop.mapred.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.junit.*;

/**
 * Tests state transition from active->standby, and manual failover
 * and failback between two jobtrackers.
 */
public class TestHAStateTransitions {
  
  private static final Log LOG = 
    LogFactory.getLog(TestHAStateTransitions.class);

  private static final Path TEST_DIR = new Path("/tmp/tst");
  
  private static final StateChangeRequestInfo REQ_INFO = new StateChangeRequestInfo(
      RequestSource.REQUEST_BY_USER_FORCED);

  private MiniMRHACluster cluster;
  private JobTrackerHADaemon jt1;
  private JobTrackerHADaemon jt2;
  private JobTrackerHAServiceTarget target1;
  private JobTrackerHAServiceTarget target2;
  private Configuration conf;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set(HAUtil.MR_HA_FENCING_METHODS_KEY,
        AlwaysSucceedFencer.class.getName());
    cluster = new MiniMRHACluster(conf);
    cluster.getJobTrackerHaDaemon(0).makeActive();
    cluster.startTaskTracker(0, 1);
    cluster.waitActive();
    
    jt1 = cluster.getJobTrackerHaDaemon(0);
    jt2 = cluster.getJobTrackerHaDaemon(1);
    target1 = new JobTrackerHAServiceTarget(jt1.getConf());
    target2 = new JobTrackerHAServiceTarget(jt2.getConf());
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }
  
  @Test(timeout=60000)
  public void testClientFailover() throws Exception {
    LOG.info("Running testClientFailover");

    // Test with client. c.f. HATestUtil.setFailoverConfigurations
    JobClient jc = new JobClient(conf);
    assertEquals("client sees jt running", JobTrackerStatus.RUNNING,
        jc.getClusterStatus().getJobTrackerStatus());

    // failover to jt2
    FailoverController fc = new FailoverController(conf, 
        RequestSource.REQUEST_BY_USER);
    fc.failover(target1, target2, false, false);
    
    cluster.waitActive();
    
    assertEquals("jt2 running", JobTrackerStatus.RUNNING,
        jt2.getJobTracker().getClusterStatus().getJobTrackerStatus());
    assertNull("jt1 not running", jt1.getJobTracker());
    
    assertEquals("client still sees jt running", JobTrackerStatus.RUNNING,
        jc.getClusterStatus().getJobTrackerStatus());
  }
  
  @Test(timeout=60000)
  public void testFailoverWhileRunningJob() throws Exception {
    LOG.info("Running testFailoverWhileRunningJob");

    // Inspired by TestRecoveryManager#testJobResubmission
    
    FileUtil.fullyDelete(new File("/tmp/tst"));
    
    // start a job on jt1
    JobConf job1 = new JobConf(conf);
    String signalFile = new Path(TEST_DIR, "signal").toString();
    UtilsForTests.configureWaitingJobConf(job1, new Path(TEST_DIR, "input"),
        new Path(TEST_DIR, "output3"), 2, 0, "test-resubmission", signalFile,
        signalFile);
    JobClient jc = new JobClient(job1);
    RunningJob rJob1 = jc.submitJob(job1);
    while (rJob1.mapProgress() < 0.5f) {
      LOG.info("Waiting for job " + rJob1.getID() + " to be 50% done: " +
          rJob1.mapProgress());
      UtilsForTests.waitFor(500);
    }
    LOG.info("Waiting for job " + rJob1.getID() + " to be 50% done: " +
        rJob1.mapProgress());
    
    // failover to jt2
    FailoverController fc = new FailoverController(conf, 
        RequestSource.REQUEST_BY_USER);
    fc.failover(target1, target2, false, false);
    
    // allow job to complete
    FileSystem fs = FileSystem.getLocal(conf);
    fs.create(new Path(TEST_DIR, "signal"));
    while (!rJob1.isComplete()) {
      LOG.info("Waiting for job " + rJob1.getID() + " to be successful: " +
          rJob1.mapProgress());
      UtilsForTests.waitFor(500);
    }
    assertTrue("Job should be successful", rJob1.isSuccessful());
  }
  
  @Test(timeout=60000)
  public void testTransitionToCurrentStateIsANop() throws Exception {
    LOG.info("Running testTransitionToCurrentStateIsANop");

    JobTracker existingJt = jt1.getJobTracker();
    jt1.getJobTrackerHAServiceProtocol().transitionToActive(REQ_INFO);
    assertSame("Should not create a new JobTracker", existingJt,
        jt1.getJobTracker());
    jt1.getJobTrackerHAServiceProtocol().transitionToStandby(REQ_INFO);
    // Transitioning to standby for a second time should not throw an exception
    jt1.getJobTrackerHAServiceProtocol().transitionToStandby(REQ_INFO);
  }

}
