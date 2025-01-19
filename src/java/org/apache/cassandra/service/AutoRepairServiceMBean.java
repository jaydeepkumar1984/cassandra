/*
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
package org.apache.cassandra.service;

import java.util.Map;
import java.util.Set;

public interface AutoRepairServiceMBean
{
    boolean getEnabled();

    String getRepairCheckInterval();

    String getHistoryClearDeleteHostsBufferInterval();

    void setHistoryClearDeleteHostsBufferInterval(String duration);

    int getRepairMaxRetries();
    
    void setRepairMaxRetries(int retries);

    String getRepairRetryBackoff();

    void setRepairRetryBackoff(String interval);

    String getRepairTaskMinDuration();

    void setRepairTaskMinDuration(String duration);

    boolean getEnabled(String repairType);

    void setEnabled(String repairType, boolean enabled);

    int getNumberOfRepairThreads(String repairType);

    void setNumberOfRepairThreads(String repairType, int repairThreads);

    Set<String> getPriorityHosts(String repairType);

    void setPriorityHosts(String repairType, Set<String> host);

    String getMinRepairInterval(String repairType);

    void setMinRepairInterval(String repairType, String minRepairInterval);

    boolean getRepairByKeyspace(String repairType);

    void setRepairByKeyspace(String repairType, boolean repairByKeyspace);

    int getSSTableUpperThreshold(String repairType);

    void setSSTableUpperThreshold(String repairType, int ssTableHigherThreshold);

    String getTableMaxRepairTime(String repairType);

    void setTableMaxRepairTime(String repairType, String autoRepairTableMaxRepairTime);

    Set<String> getIgnoreDCs(String repairType);

    void setIgnoreDCs(String repairType, Set<String> ignorDCs);

    boolean getRepairPrimaryTokenRangeOnly(String repairType);

    void setRepairPrimaryTokenRangeOnly(String repairType, boolean primaryTokenRangeOnly);

    int getParallelRepairPercentage(String repairType);

    void setParallelRepairPercentage(String repairType, int percentage);

    int getParallelRepairCount(String repairType);

    void setParallelRepairCount(String repairType, int count);

    boolean getMaterializedViewRepairEnabled(String repairType);

    void setMaterializedViewRepairEnabled(String repairType, boolean enabled);

    String getRepairSessionTimeout(String repairType);

    void setRepairSessionTimeout(String repairType, String timeout);

    Map<String, String> getTokenRangeSplitterInstance(String repairType);

    void setTokenRangeSplitterInstance(String repairType, String key, String value);

    Set<String> getOnGoingRepairHostIds(String rType);

    boolean getForceRepairNewNode(String repairType);

    String getInitialSchedulerDelay(String repairType);

    String getTokenRangeSplitter(String repairType);

    void setForceRepair(String repairType, Set<String> host);

    void startScheduler();
}
