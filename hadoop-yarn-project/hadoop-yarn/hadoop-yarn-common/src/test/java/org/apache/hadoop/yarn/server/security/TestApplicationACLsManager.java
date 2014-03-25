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
package org.apache.hadoop.yarn.server.security;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;

public class TestApplicationACLsManager {

  @Test
  public void testCheckAccessNoApplication() throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, " ");

    ApplicationACLsManager applicationACLsManager =
        new ApplicationACLsManager(conf);

    UserGroupInformation user = UserGroupInformation.createRemoteUser("user");
    ApplicationId appId = ApplicationId.newInstance(123456, 1);
    assertFalse(applicationACLsManager.checkAccess(user,
        ApplicationAccessType.VIEW_APP,
        UserGroupInformation.getCurrentUser().getUserName(), appId));
  }
}
