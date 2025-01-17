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
package org.apache.cassandra.tools.nodetool;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import io.airlift.airline.Command;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import java.io.PrintStream;
import java.util.Map;
import java.util.Set;

@Command(name = "getautorepairconfig", description = "Print autorepair configurations")
public class GetAutoRepairConfig extends NodeToolCmd
{
    @VisibleForTesting
    protected static PrintStream out = System.out;

    @Override
    public void execute(NodeProbe probe)
    {
//        AutoRepairConfig config = probe.getAutoRepairConfig();
        if (!probe.getEnabled())
        {
            out.println("Auto-repair is not enabled");
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("repair scheduler configuration:");
        appendConfig(sb, "repair_check_interval", probe.getRepairCheckInterval());
        appendConfig(sb, "repair_max_retries", probe.getRepairMaxRetries());
        appendConfig(sb, "repair_retry_backoff", probe.getRepairRetryBackoff());
        appendConfig(sb, "repair_task_min_duration", probe.getRepairTaskMinDuration());
        appendConfig(sb, "history_clear_delete_hosts_buffer_interval", probe.getHistoryClearDeleteHostsBufferInterval());
        for (RepairType repairType : RepairType.values())
        {
            sb.append(formatRepairTypeConfig(probe, repairType));
        }

        out.println(sb);
    }

    private String formatRepairTypeConfig(NodeProbe probe, RepairType repairType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("\nconfiguration for repair_type: ").append(repairType.getConfigName());
        sb.append("\n\tenabled: ").append(probe.getEnabled(repairType));
        // Only show configuration if enabled
        if (probe.getEnabled(repairType))
        {
            Set<InetAddressAndPort> priorityHosts = probe.getPriorityHosts(repairType);
            if (!priorityHosts.isEmpty())
            {
                appendConfig(sb, "priority_hosts", Joiner.on(',').skipNulls().join(priorityHosts));
            }

            appendConfig(sb , "min_repair_interval", probe.getMinRepairInterval(repairType));
            appendConfig(sb , "repair_by_keyspace", probe.getRepairByKeyspace(repairType));
            appendConfig(sb , "number_of_repair_threads", probe.getNumberOfRepairThreads(repairType));
            appendConfig(sb , "sstable_upper_threshold", probe.getSSTableUpperThreshold(repairType));
            appendConfig(sb , "table_max_repair_time", probe.getTableMaxRepairTime(repairType));
            appendConfig(sb , "ignore_dcs", probe.getIgnoreDCs(repairType));
            appendConfig(sb , "repair_primary_token_range_only", probe.getRepairPrimaryTokenRangeOnly(repairType));
            appendConfig(sb , "parallel_repair_count", probe.getParallelRepairCount(repairType));
            appendConfig(sb , "parallel_repair_percentage", probe.getParallelRepairPercentage(repairType));
            appendConfig(sb , "materialized_view_repair_enabled", probe.getMaterializedViewRepairEnabled(repairType));
            appendConfig(sb , "initial_scheduler_delay", probe.getInitialSchedulerDelay(repairType));
            appendConfig(sb , "repair_session_timeout", probe.getRepairSessionTimeout(repairType));
            appendConfig(sb , "force_repair_new_node", probe.getForceRepairNewNode(repairType));

            final String splitterClassName = probe.getTokenRangeSplitter(repairType);
            //final String splitterClassName =  splitterClass.class_name != null ? splitterClass.class_name : AutoRepairConfig.DEFAULT_SPLITTER.getName();
            appendConfig(sb, "token_range_splitter", splitterClassName);
            Map<String, String> tokenRangeSplitterParameters = probe.getTokenRangeSplitterInstance(repairType);
            if (!tokenRangeSplitterParameters.isEmpty())
            {
                for (Map.Entry<String, String> param : tokenRangeSplitterParameters.entrySet())
                {
                    appendConfig(sb, String.format("token_range_splitter.%s", param.getKey()), param.getValue());
                }
            }
        }

        return sb.toString();
    }

    private <T> void appendConfig(StringBuilder sb, String config, T value)
    {
        sb.append(String.format("%s%s: %s", "\n\t", config, value));
    }
}
