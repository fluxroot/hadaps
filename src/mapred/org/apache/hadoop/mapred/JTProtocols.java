package org.apache.hadoop.mapred;

import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.tools.GetUserMappingsProtocol;

public interface JTProtocols extends
  InterTrackerProtocol,
  JobSubmissionProtocol, TaskTrackerManager, RefreshUserMappingsProtocol,
  GetUserMappingsProtocol,
  RefreshAuthorizationPolicyProtocol, AdminOperationsProtocol
{

}
