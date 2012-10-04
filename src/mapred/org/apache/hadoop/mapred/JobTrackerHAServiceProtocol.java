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

import static org.apache.hadoop.util.ExitUtil.terminate;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ExitUtil.ExitException;

public class JobTrackerHAServiceProtocol implements HAServiceProtocol {
  
  private static final Log LOG =
    LogFactory.getLog(JobTrackerHAServiceProtocol.class);

  private Configuration conf;
  private HAServiceState haState = HAServiceState.STANDBY;
  private JobTracker jt;
  private Thread jtThread;
  private JobTrackerHAHttpRedirector httpRedirector;
  
  public JobTrackerHAServiceProtocol(Configuration conf) {
    this.conf = conf;
    this.httpRedirector = new JobTrackerHAHttpRedirector(conf);
    try {
      httpRedirector.start();
    } catch (Throwable t) {
      doImmediateShutdown(t);
    }
  }
  
  public JobTracker getJobTracker() {
    return jt;
  }
  
  @VisibleForTesting
  Thread getJobTrackerThread() {
    return jtThread;
  }

  @Override
  public HAServiceStatus getServiceStatus() throws AccessControlException,
      IOException {
    HAServiceStatus ret = new HAServiceStatus(haState);
    if (haState == HAServiceState.STANDBY || haState == HAServiceState.ACTIVE) {
      ret.setReadyToBecomeActive();
    } else {
      ret.setNotReadyToBecomeActive("State is " + haState);
    }
    return ret;
  }

  @Override
  public void monitorHealth() throws HealthCheckFailedException {
    if (haState == HAServiceState.ACTIVE && jtThreadIsNotAlive()) {
      throw new HealthCheckFailedException("The JobTracker thread is not running");
    }
  }

  private boolean jtThreadIsNotAlive() {
    return jtThread == null || !jtThread.isAlive();
  }

  @Override
  public void transitionToActive(StateChangeRequestInfo reqInfo)
      throws ServiceFailedException, AccessControlException, IOException {
    if (haState == HAServiceState.ACTIVE) {
      LOG.info("Already in active state.");
      return;
    }
    LOG.info("Transitioning to active");
    try {
      httpRedirector.stop();
      JobConf jtConf = new JobConf(conf);
      // Update the conf for the JT so the address is resolved
      HAUtil.setJtRpcAddress(jtConf);
      jt = JobTracker.startTracker(jtConf);
    } catch (Throwable t) {
      doImmediateShutdown(t);
    }
    jtThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          jt.offerService();
        } catch (Throwable t) {
          doImmediateShutdown(t);
        }
      }
    });
    jtThread.start();
    haState = HAServiceState.ACTIVE;
    LOG.info("Transitioned to active");
  }

  @Override
  public void transitionToStandby(StateChangeRequestInfo reqInfo)
      throws ServiceFailedException, AccessControlException, IOException {
    if (haState == HAServiceState.STANDBY) {
      LOG.info("Already in standby state.");
      return;
    }
    LOG.info("Transitioning to standby");
    try {
      if (jt != null) {
        jt.close();
      }
      if (jtThread != null) {
        jtThread.join();
      }
      httpRedirector.start();
    } catch (Throwable t) {
      doImmediateShutdown(t);
    }
    jt = null;
    jtThread = null;
    haState = HAServiceState.STANDBY;
    LOG.info("Transitioned to standby");
  }
  
  public void stop() {
    LOG.info("Stopping");
    try {
      if (jt != null) {
        jt.close();
      }
      if (jtThread != null) {
        jtThread.join();
      }
      httpRedirector.stop();
    } catch (Throwable t) {
      doImmediateShutdown(t);
    }
    jt = null;
    jtThread = null;
    haState = HAServiceState.STANDBY;
    LOG.info("Stopped");
  }
  
  /**
   * Shutdown the JT immediately in an ungraceful way. Used when it would be
   * unsafe for the JT to continue operating, e.g. during a failed HA state
   * transition.
   * 
   * @param t exception which warrants the shutdown. Printed to the JT log
   *          before exit.
   * @throws ExitException thrown only for testing.
   */
  private synchronized void doImmediateShutdown(Throwable t)
      throws ExitException {
    String message = "Error encountered requiring JT shutdown. " +
        "Shutting down immediately.";
    try {
      LOG.fatal(message, t);
    } catch (Throwable ignored) {
      // This is unlikely to happen, but there's nothing we can do if it does.
    }
    terminate(1, t);
  }

}
