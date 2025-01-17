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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils;
import org.apache.cassandra.utils.MBeanWrapper;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;

public class AutoRepairService implements AutoRepairServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=AutoRepairService";

    @VisibleForTesting
    protected AutoRepairConfig config;

    public static final AutoRepairService instance = new AutoRepairService();

    @VisibleForTesting
    protected AutoRepairService()
    {
    }

    public static void setup()
    {
        instance.config = DatabaseDescriptor.getAutoRepairConfig();
    }

    static
    {
        MBeanWrapper.instance.registerMBean(instance, MBEAN_NAME);
    }

    public void checkCanRun(RepairType repairType)
    {
        if (!config.getEnabled())
            throw new ConfigurationException("Auto-repair scheduller is disabled.");

        if (repairType != RepairType.INCREMENTAL)
            return;

        if (DatabaseDescriptor.isMaterializedViewsOnRepairEnabled())
            throw new ConfigurationException("Cannot run incremental repair while materialized view replay is enabled. Set materialized_views_on_repair_enabled to false.");

        if (DatabaseDescriptor.isCDCOnRepairEnabled())
            throw new ConfigurationException("Cannot run incremental repair while CDC replay is enabled. Set cdc_on_repair_enabled to false.");
    }

    //@Override
    public AutoRepairConfig getAutoRepairConfig()
    {
        return config;
    }

    @Override
    public boolean getEnabled()
    {
        return config.getEnabled();
    }

    @Override
    public String getRepairCheckInterval()
    {
        return config.getRepairCheckInterval().toString();
    }

    @Override
    public boolean getEnabled(RepairType repairType)
    {
        return config.getEnabled(repairType);
    }

    @Override
    public void setEnabled(RepairType repairType, boolean enabled)
    {
        checkCanRun(repairType);
        config.setEnabled(repairType, enabled);
    }

    @Override
    public int getNumberOfRepairThreads(RepairType repairType)
    {
        return config.getNumberOfRepairThreads(repairType);
    }

    @Override
    public void setNumberOfRepairThreads(RepairType repairType, int repairThreads)
    {
        config.setNumberOfRepairThreads(repairType, repairThreads);
    }

    @Override
    public Set<InetAddressAndPort> getPriorityHosts(RepairType repairType)
    {
        return AutoRepairUtils.getPriorityHosts(repairType);
    }

    @Override
    public void setPriorityHosts(RepairType repairType, Set<InetAddressAndPort> hosts)
    {
        AutoRepairUtils.addPriorityHosts(repairType, hosts);
    }

    @Override
    public Set<InetAddressAndPort> getForceRepairForHosts(RepairType repairType)
    {
        return null;
    }

    @Override
    public void setForceRepair(RepairType repairType, Set<InetAddressAndPort> hosts)
    {
        AutoRepairUtils.setForceRepair(repairType, hosts);
    }

    @Override
    public String getMinRepairInterval(RepairType repairType)
    {
        return config.getMinRepairInterval(repairType).toString();
    }

    @Override
    public void setMinRepairInterval(RepairType repairType, String minRepairInterval)
    {
        config.setMinRepairInterval(repairType, minRepairInterval);
    }

    @Override
    public boolean getRepairByKeyspace(RepairType repairType)
    {
        return config.getRepairByKeyspace(repairType);
    }

    @Override
    public void setRepairByKeyspace(RepairType repairType, boolean repairByKeyspace)
    {
        config.setRepairByKeyspace(repairType, repairByKeyspace);
    }

    @Override
    public void startScheduler()
    {
        config.startScheduler();
    }

    @Override
    public String getHistoryClearDeleteHostsBufferInterval()
    {
        return config.getHistoryClearDeleteHostsBufferInterval().toString();
    }

    @Override
    public void setHistoryClearDeleteHostsBufferInterval(String duration)
    {
        config.setHistoryClearDeleteHostsBufferInterval(duration);
    }

    @Override
    public int getRepairMaxRetries()
    {
        return config.getRepairMaxRetries();
    }

    @Override
    public void setRepairMaxRetries(int retries)
    {
        config.setRepairMaxRetries(retries);
    }

    @Override
    public String getRepairRetryBackoff()
    {
        return config.getRepairRetryBackoff().toString();
    }

    @Override
    public void setRepairRetryBackoff(String interval)
    {
        config.setRepairRetryBackoff(interval);
    }

    @Override
    public String getRepairTaskMinDuration()
    {
        return config.getRepairTaskMinDuration().toString();
    }

    @Override
    public void setRepairTaskMinDuration(String duration)
    {
        config.setRepairTaskMinDuration(duration);
    }


    @Override
    public int getSSTableUpperThreshold(RepairType repairType)
    {
        return config.getSSTableUpperThreshold(repairType);
    }


    @Override
    public void setSSTableUpperThreshold(RepairType repairType, int sstableHigherThreshold)
    {
        config.setSSTableUpperThreshold(repairType, sstableHigherThreshold);
    }

    @Override
    public String getTableMaxRepairTime(RepairType repairType)
    {
        return config.getTableMaxRepairTime(repairType).toString();
    }

    @Override
    public void setTableMaxRepairTime(RepairType repairType, String autoRepairTableMaxRepairTime)
    {
        config.setTableMaxRepairTime(repairType, autoRepairTableMaxRepairTime);
    }

    @Override
    public Set<String> getIgnoreDCs(RepairType repairType)
    {
        return config.getIgnoreDCs(repairType);
    }

    @Override
    public void setIgnoreDCs(RepairType repairType, Set<String> ignoreDCs)
    {
        config.setIgnoreDCs(repairType, ignoreDCs);
    }

    @Override
    public boolean getRepairPrimaryTokenRangeOnly(RepairType repairType)
    {
        return config.getRepairPrimaryTokenRangeOnly(repairType);
    }

    @Override
    public void setRepairPrimaryTokenRangeOnly(RepairType repairType, boolean primaryTokenRangeOnly)
    {
        config.setRepairPrimaryTokenRangeOnly(repairType, primaryTokenRangeOnly);
    }

    @Override
    public int getParallelRepairPercentage(RepairType repairType)
    {
        return config.getParallelRepairPercentage(repairType);
    }

    @Override
    public void setParallelRepairPercentage(RepairType repairType, int percentage)
    {
        config.setParallelRepairPercentage(repairType, percentage);
    }

    @Override
    public int getParallelRepairCount(RepairType repairType)
    {
        return config.getParallelRepairCount(repairType);
    }

    @Override
    public void setParallelRepairCount(RepairType repairType, int count)
    {
        config.setParallelRepairCount(repairType, count);
    }

    @Override
    public boolean getMaterializedViewRepairEnabled(RepairType repairType)
    {
        return config.getMaterializedViewRepairEnabled(repairType);
    }

    @Override
    public void setMaterializedViewRepairEnabled(RepairType repairType, boolean enabled)
    {
        config.setMaterializedViewRepairEnabled(repairType, enabled);
    }

    @Override
    public String getRepairSessionTimeout(RepairType repairType)
    {
        return config.getRepairSessionTimeout(repairType).toString();
    }

    @Override
    public void setRepairSessionTimeout(RepairType repairType, String timeout)
    {
        config.setRepairSessionTimeout(repairType, timeout);
    }

    @Override
    public Set<String> getOnGoingRepairHostIds(RepairType rType)
    {
        Set<String> hostIds = new HashSet<>();
        List<AutoRepairUtils.AutoRepairHistory> histories = AutoRepairUtils.getAutoRepairHistory(rType);
        if (histories == null)
        {
            return hostIds;
        }
        AutoRepairUtils.CurrentRepairStatus currentRepairStatus = new AutoRepairUtils.CurrentRepairStatus(histories, AutoRepairUtils.getPriorityHostIds(rType));
        for (UUID id : currentRepairStatus.hostIdsWithOnGoingRepair)
        {
            hostIds.add(id.toString());
        }
        for (UUID id : currentRepairStatus.hostIdsWithOnGoingForceRepair)
        {
            hostIds.add(id.toString());
        }
        return hostIds;
    }

    @Override
    public Map<String, String> getTokenRangeSplitterInstance(RepairType repairType)
    {
        return config.getTokenRangeSplitterInstance(repairType).getParameters();
    }

    @Override
    public void getTokenRangeSplitterInstance(RepairType repairType, String key, String value)
    {
        config.getTokenRangeSplitterInstance(repairType).setParameter(key, value);
    }

    @Override
    public boolean getForceRepairNewNode(RepairType repairType)
    {
        return config.getForceRepairNewNode(repairType);
    }

    @Override
    public String getInitialSchedulerDelay(RepairType repairType)
    {
        return config.getInitialSchedulerDelay(repairType).toString();
    }

    @Override
    public String getTokenRangeSplitter(RepairType repairType)
    {
        final ParameterizedClass splitterClass = config.getTokenRangeSplitter(repairType);
        final String splitterClassName =  splitterClass.class_name != null ? splitterClass.class_name : AutoRepairConfig.DEFAULT_SPLITTER.getName();
        return splitterClassName;
    }
}
