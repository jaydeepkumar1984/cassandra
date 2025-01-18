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

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
    public boolean getEnabled(String repairType)
    {
        return config.getEnabled(RepairType.fromString(repairType));
    }

    @Override
    public void setEnabled(String repairType, boolean enabled)
    {
        checkCanRun(RepairType.fromString(repairType));
        config.setEnabled(RepairType.fromString(repairType), enabled);
    }

    @Override
    public int getNumberOfRepairThreads(String repairType)
    {
        return config.getNumberOfRepairThreads(RepairType.fromString(repairType));
    }

    @Override
    public void setNumberOfRepairThreads(String repairType, int repairThreads)
    {
        config.setNumberOfRepairThreads(RepairType.fromString(repairType), repairThreads);
    }

    @Override
    public Set<String> getPriorityHosts(String repairType)
    {
        return AutoRepairUtils.getPriorityHosts(RepairType.fromString(repairType));
    }

    @Override
    public void setPriorityHosts(String repairType, Set<String> hostsStr)
    {
        AutoRepairUtils.addPriorityHosts(RepairType.fromString(repairType), convertToHosts(hostsStr));
    }

    @Override
    public String getMinRepairInterval(String repairType)
    {
        return config.getMinRepairInterval(RepairType.fromString(repairType)).toString();
    }

    @Override
    public void setMinRepairInterval(String repairType, String minRepairInterval)
    {
        config.setMinRepairInterval(RepairType.fromString(repairType), minRepairInterval);
    }

    @Override
    public boolean getRepairByKeyspace(String repairType)
    {
        return config.getRepairByKeyspace(RepairType.fromString(repairType));
    }

    @Override
    public void setRepairByKeyspace(String repairType, boolean repairByKeyspace)
    {
        config.setRepairByKeyspace(RepairType.fromString(repairType), repairByKeyspace);
    }

    @Override
    public int getSSTableUpperThreshold(String repairType)
    {
        return config.getSSTableUpperThreshold(RepairType.fromString(repairType));
    }


    @Override
    public void setSSTableUpperThreshold(String repairType, int sstableHigherThreshold)
    {
        config.setSSTableUpperThreshold(RepairType.fromString(repairType), sstableHigherThreshold);
    }

    @Override
    public String getTableMaxRepairTime(String repairType)
    {
        return config.getTableMaxRepairTime(RepairType.fromString(repairType)).toString();
    }

    @Override
    public void setTableMaxRepairTime(String repairType, String autoRepairTableMaxRepairTime)
    {
        config.setTableMaxRepairTime(RepairType.fromString(repairType), autoRepairTableMaxRepairTime);
    }

    @Override
    public Set<String> getIgnoreDCs(String repairType)
    {
        return config.getIgnoreDCs(RepairType.fromString(repairType));
    }

    @Override
    public void setIgnoreDCs(String repairType, Set<String> ignoreDCs)
    {
        config.setIgnoreDCs(RepairType.fromString(repairType), ignoreDCs);
    }

    @Override
    public boolean getRepairPrimaryTokenRangeOnly(String repairType)
    {
        return config.getRepairPrimaryTokenRangeOnly(RepairType.fromString(repairType));
    }

    @Override
    public void setRepairPrimaryTokenRangeOnly(String repairType, boolean primaryTokenRangeOnly)
    {
        config.setRepairPrimaryTokenRangeOnly(RepairType.fromString(repairType), primaryTokenRangeOnly);
    }

    @Override
    public int getParallelRepairPercentage(String repairType)
    {
        return config.getParallelRepairPercentage(RepairType.fromString(repairType));
    }

    @Override
    public void setParallelRepairPercentage(String repairType, int percentage)
    {
        config.setParallelRepairPercentage(RepairType.fromString(repairType), percentage);
    }

    @Override
    public int getParallelRepairCount(String repairType)
    {
        return config.getParallelRepairCount(RepairType.fromString(repairType));
    }

    @Override
    public void setParallelRepairCount(String repairType, int count)
    {
        config.setParallelRepairCount(RepairType.fromString(repairType), count);
    }

    @Override
    public boolean getMaterializedViewRepairEnabled(String repairType)
    {
        return config.getMaterializedViewRepairEnabled(RepairType.fromString(repairType));
    }

    @Override
    public void setMaterializedViewRepairEnabled(String repairType, boolean enabled)
    {
        config.setMaterializedViewRepairEnabled(RepairType.fromString(repairType), enabled);
    }

    @Override
    public String getRepairSessionTimeout(String repairType)
    {
        return config.getRepairSessionTimeout(RepairType.fromString(repairType)).toString();
    }

    @Override
    public void setRepairSessionTimeout(String repairType, String timeout)
    {
        config.setRepairSessionTimeout(RepairType.fromString(repairType), timeout);
    }

    @Override
    public Map<String, String> getTokenRangeSplitterInstance(String repairType)
    {
        return config.getTokenRangeSplitterInstance(RepairType.fromString(repairType)).getParameters();
    }

    @Override
    public void setTokenRangeSplitterInstance(String repairType, String key, String value)
    {
        config.getTokenRangeSplitterInstance(RepairType.fromString(repairType)).setParameter(key, value);
    }

    @Override
    public Set<String> getOnGoingRepairHostIds(String repairType)
    {
        Set<String> hostIds = new HashSet<>();
        List<AutoRepairUtils.AutoRepairHistory> histories = AutoRepairUtils.getAutoRepairHistory(RepairType.fromString(repairType));
        if (histories == null)
        {
            return hostIds;
        }
        AutoRepairUtils.CurrentRepairStatus currentRepairStatus = new AutoRepairUtils.CurrentRepairStatus(histories, AutoRepairUtils.getPriorityHostIds(RepairType.fromString(repairType)));
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
    public boolean getForceRepairNewNode(String repairType)
    {
        return config.getForceRepairNewNode(RepairType.fromString(repairType));
    }

    @Override
    public String getInitialSchedulerDelay(String repairType)
    {
        return config.getInitialSchedulerDelay(RepairType.fromString(repairType)).toString();
    }

    @Override
    public String getTokenRangeSplitter(String repairType)
    {
        final ParameterizedClass splitterClass = config.getTokenRangeSplitter(RepairType.fromString(repairType));
        final String splitterClassName = splitterClass.class_name != null ? splitterClass.class_name : AutoRepairConfig.DEFAULT_SPLITTER.getName();
        return splitterClassName;
    }

    @Override
    public void setForceRepair(String repairType, Set<String> hostsStr)
    {
        AutoRepairUtils.setForceRepair(RepairType.fromString(repairType), convertToHosts(hostsStr));
    }

    @Override
    public void startScheduler()
    {
        config.startScheduler();
    }

    private Set<InetAddressAndPort> convertToHosts(Set<String> hostsStr)
    {
        return hostsStr.stream()
                       .map(hostname -> {
                           try
                           {
                               return InetAddressAndPort.getByName(hostname);
                           }
                           catch (UnknownHostException e)
                           {
                               throw new RuntimeException(e);
                           }
                       })
                       .collect(Collectors.toSet());
    }
}
