/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.security;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Facilitates getting UserGroupInformation configured for a given user.
 */
public interface UGIProvider {

  /**
   * Looks up the Kerberos principal to be impersonated for a given user and returns a {@link UserGroupInformation}
   * for that principal.
   *
   * @param impersonationInfo information specifying how to create the UGI
   * @return the {@link UserGroupInformation} for the configured user
   */
  UserGroupInformation getConfiguredUGI(ImpersonationInfo impersonationInfo) throws IOException;
}
