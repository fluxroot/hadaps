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
import java.net.URL;

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
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.mapred.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.junit.*;

/**
 * Tests web UI redirect from standby to active jobtracker.
 */
public class TestHAWebUI {
  
  private static final Log LOG = 
    LogFactory.getLog(TestHAWebUI.class);

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
  public void testRedirect() throws Exception {

    // both jobtracker addresses should serve up the jobtracker page
    // regardless of state
    checkJobTrackerPage("jt1");
    checkJobTrackerPage("jt2");

    // failover to jt2
    FailoverController fc = new FailoverController(conf, 
        RequestSource.REQUEST_BY_USER);
    fc.failover(target1, target2, false, false);
    
    cluster.waitActive();
    
    checkJobTrackerPage("jt1");
    checkJobTrackerPage("jt2");
  }
  
  private void checkJobTrackerPage(String jtId) throws IOException {
    String redirectAddress = conf.get(HAUtil.addKeySuffixes(
        HAUtil.MR_HA_JOBTRACKER_HTTP_REDIRECT_ADDRESS_KEY, "logicaljt", jtId));
    URL url = new URL("http://" + redirectAddress + "/jobtracker.jsp");
    String page = DFSTestUtil.urlGet(url);
    assertTrue(page.contains("Hadoop Map/Reduce Administration"));
  }

}
