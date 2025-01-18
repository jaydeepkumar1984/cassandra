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

    public String getHistoryClearDeleteHostsBufferInterval();
    public void setHistoryClearDeleteHostsBufferInterval(String duration);

    public int getRepairMaxRetries();
    public void setRepairMaxRetries(int retries);

    public String getRepairRetryBackoff();
    public void setRepairRetryBackoff(String interval);

    public String getRepairTaskMinDuration();
    public void setRepairTaskMinDuration(String duration);

    public boolean getEnabled(String repairType);
    /**
     * Enable or disable auto-repair for a given repair type
     */
    public void setEnabled(String repairType, boolean enabled);

    public int getNumberOfRepairThreads(String repairType);

    public void setNumberOfRepairThreads(String repairType, int repairThreads);

    public Set<String> getPriorityHosts(String repairType);

    void setPriorityHosts(String repairType, Set<String> host);

    public void setForceRepair(String repairType, Set<String> host);

    public String getMinRepairInterval(String repairType);

    public void setMinRepairInterval(String repairType, String minRepairInterval);


    boolean getRepairByKeyspace(String repairType);

    void setRepairByKeyspace(String repairType, boolean repairByKeyspace);


    void startScheduler();


    public int getSSTableUpperThreshold(String repairType);
    public void setSSTableUpperThreshold(String repairType, int ssTableHigherThreshold);

    public String getTableMaxRepairTime(String repairType);
    public void setTableMaxRepairTime(String repairType, String autoRepairTableMaxRepairTime);

    public Set<String> getIgnoreDCs(String repairType);
    public void setIgnoreDCs(String repairType, Set<String> ignorDCs);

    public boolean getRepairPrimaryTokenRangeOnly(String repairType);
    public void setRepairPrimaryTokenRangeOnly(String repairType, boolean primaryTokenRangeOnly);

    public int getParallelRepairPercentage(String repairType);
    public void setParallelRepairPercentage(String repairType, int percentage);

    public int getParallelRepairCount(String repairType);
    public void setParallelRepairCount(String repairType, int count);

    public boolean getMaterializedViewRepairEnabled(String repairType);
    public void setMaterializedViewRepairEnabled(String repairType, boolean enabled);

    public String getRepairSessionTimeout(String repairType);
    public void setRepairSessionTimeout(String repairType, String timeout);

    public Set<String> getOnGoingRepairHostIds(String rType);

    public Map<String, String> getTokenRangeSplitterInstance(String repairType);

    public void setTokenRangeSplitterInstance(String repairType, String key, String value);

    boolean getForceRepairNewNode(String repairType);

    String getInitialSchedulerDelay(String repairType);

    String getTokenRangeSplitter(String repairType);
}
