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

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;

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

    public boolean getEnabled(RepairType repairType);
    /**
     * Enable or disable auto-repair for a given repair type
     */
    public void setEnabled(RepairType repairType, boolean enabled);

    public int getNumberOfRepairThreads(RepairType repairType);

    public void setNumberOfRepairThreads(RepairType repairType, int repairThreads);

    public Set<InetAddressAndPort> getPriorityHosts(RepairType repairType);

    void setPriorityHosts(RepairType repairType, Set<InetAddressAndPort> host);

    public Set<InetAddressAndPort> getForceRepairForHosts(RepairType repairType);

    public void setForceRepair(RepairType repairType, Set<InetAddressAndPort> host);

    public String getMinRepairInterval(RepairType repairType);

    public void setMinRepairInterval(RepairType repairType, String minRepairInterval);


    boolean getRepairByKeyspace(RepairType repairType);

    void setRepairByKeyspace(RepairType repairType, boolean repairByKeyspace);


    void startScheduler();


    public int getSSTableUpperThreshold(RepairType repairType);
    public void setSSTableUpperThreshold(RepairType repairType, int ssTableHigherThreshold);

    public String getTableMaxRepairTime(RepairType repairType);
    public void setTableMaxRepairTime(RepairType repairType, String autoRepairTableMaxRepairTime);

    public Set<String> getIgnoreDCs(RepairType repairType);
    public void setIgnoreDCs(RepairType repairType, Set<String> ignorDCs);

    public boolean getRepairPrimaryTokenRangeOnly(RepairType repairType);
    public void setRepairPrimaryTokenRangeOnly(RepairType repairType, boolean primaryTokenRangeOnly);

    public int getParallelRepairPercentage(RepairType repairType);
    public void setParallelRepairPercentage(RepairType repairType, int percentage);

    public int getParallelRepairCount(RepairType repairType);
    public void setParallelRepairCount(RepairType repairType, int count);

    public boolean getMaterializedViewRepairEnabled(RepairType repairType);
    public void setMaterializedViewRepairEnabled(RepairType repairType, boolean enabled);

    //public AutoRepairConfig getAutoRepairConfig();
//    public Map<String, String> getTopLevelSettings();
//
//    public Map<String, Map<String, String>> getRepairSpecificSettings();

    public String getRepairSessionTimeout(RepairType repairType);
    public void setRepairSessionTimeout(RepairType repairType, String timeout);

    public Set<String> getOnGoingRepairHostIds(RepairType rType);

    public Map<String, String> getTokenRangeSplitterInstance(RepairType repairType);

    public void getTokenRangeSplitterInstance(RepairType repairType, String key, String value);

    boolean getForceRepairNewNode(RepairType repairType);

    String getInitialSchedulerDelay(RepairType repairType);

    String getTokenRangeSplitter(RepairType repairType);
}
