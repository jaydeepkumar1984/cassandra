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

package org.apache.cassandra.repair;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.cassandra.repair.autorepair.AutoRepairTokenRangeDefault;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.repair.state.AutoRepairState;
import org.apache.cassandra.repair.state.AutoRepairStateFactory;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.repair.AutoRepairUtilsV2.RepairTurn;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.repair.AutoRepairUtilsV2.RepairTurn.MY_TURN;
import static org.apache.cassandra.repair.AutoRepairUtilsV2.RepairTurn.MY_TURN_DUE_TO_PRIORITY;
import static org.apache.cassandra.repair.AutoRepairUtilsV2.RepairTurn.MY_TURN_FORCE_REPAIR;

// TODO: add class documentation (SO-28898)
public class AutoRepairV2 {
    // Initial delay for repair session to start after setup
    final static long INITIAL_REPAIR_DELAY_SEC = 30;

    private static final Logger logger = LoggerFactory.getLogger(AutoRepairV2.class);

    @VisibleForTesting
    protected static Supplier<Long> timeFunc = System::currentTimeMillis;

    public static AutoRepairV2 instance = new AutoRepairV2();

    @VisibleForTesting
    protected final Map<AutoRepairConfig.RepairType, ScheduledExecutorPlus> repairExecutors;
    @VisibleForTesting
    protected final Map<AutoRepairConfig.RepairType, AutoRepairState> repairStates;


    @VisibleForTesting
    protected AutoRepairV2() {
        repairExecutors = new EnumMap<>(AutoRepairConfig.RepairType.class);
        repairStates = new EnumMap<>(AutoRepairConfig.RepairType.class);
        for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values()) {
            repairExecutors.put(repairType, executorFactory().scheduled(false, "AutoRepair-Repair-" + repairType, Thread.NORM_PRIORITY));
            repairStates.put(repairType, AutoRepairStateFactory.getAutoRepairState(repairType));
        }
    }

    public void setup() {
        verifyIsSafeToEnable();

        AutoRepairConfig config = DatabaseDescriptor.getAutoRepairConfig();
        AutoRepairService.setup();
        AutoRepairUtilsV2.setup();

        for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values()) {
            repairExecutors.get(repairType).scheduleWithFixedDelay(
                    () -> repair(repairType, 60000),
                    INITIAL_REPAIR_DELAY_SEC,
                    config.getRepairCheckIntervalInSec(),
                    TimeUnit.SECONDS);
        }
    }

    @VisibleForTesting
    protected void verifyIsSafeToEnable() {
        AutoRepairConfig config = DatabaseDescriptor.getAutoRepairConfig();
        if (config.isAutoRepairEnabled(AutoRepairConfig.RepairType.incremental) &&
                (DatabaseDescriptor.getMaterializedViewsEnabled() || DatabaseDescriptor.isCDCEnabled()))
            throw new ConfigurationException("Cannot enable incremental repair with materialized views or CDC enabled");
    }

    // repairAsync runs a repair session of the given type asynchronously.
    public void repairAsync(AutoRepairConfig.RepairType repairType, long millisToWait) {
        repairExecutors.get(repairType).submit(() -> repair(repairType, millisToWait));
    }

    // repair runs a repair session of the given type synchronously.
    public void repair(AutoRepairConfig.RepairType repairType, long millisToWait) {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        if (!config.isAutoRepairEnabled(repairType)) {
            logger.debug("Auto-repair is disabled for repair type {}", repairType);
            return;
        }


        AutoRepairState repairState = repairStates.get(repairType);

        try {
            String localDC = DatabaseDescriptor.getLocalDataCenter();
            if (config.getIgnoreDCs(repairType).contains(localDC)) {
                logger.info("Not running repair as this node belongs to datacenter {}", localDC);
                return;
            }

            // refresh the longest unrepaired node
            repairState.setLongestUnrepairedNode(AutoRepairUtilsV2.getHostWithLongestUnrepairTime(repairType));

            //consistency level to use for local query
            UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
            RepairTurn turn = AutoRepairUtilsV2.myTurnToRunRepair(repairType, myId);
            if (turn == MY_TURN || turn == MY_TURN_DUE_TO_PRIORITY || turn == MY_TURN_FORCE_REPAIR) {
                repairState.recordTurn(turn);
                // For normal auto repair, we will use primary range only repairs (Repair with -pr option).
                // For some cases, we may set the auto_repair_primary_token_range_only flag to false then we will do repair
                // without -pr. We may also do force repair for certain node that we want to repair all the data on one node
                // When doing force repair, we want to repair without -pr.
                boolean primaryRangeOnly = config.getRepairPrimaryTokenRangeOnly(repairType)
                        && turn != MY_TURN_FORCE_REPAIR;
                repairState.setTotalTablesConsideredForRepair(0);
                if (repairState.getLastRepairTime() != 0) {
                    /** check if it is too soon to run repair. one of the reason we
                     * should not run frequent repair is because repair triggers
                     * memtable flush
                     */
                    long timeElapsedSinceLastRepairInHours = TimeUnit.MILLISECONDS.toHours(timeFunc.get() - repairState.getLastRepairTime());
                    if (timeElapsedSinceLastRepairInHours < config.getRepairMinIntervalInHours(repairType)) {
                        logger.info("Too soon to run repair, last repair was done {} hour(s) ago",
                                timeElapsedSinceLastRepairInHours);
                        return;
                    }
                }

                long startTime = timeFunc.get();
                logger.info("My host id: {}, my turn to run repair...repair primary-ranges only? {}", myId,
                        config.getRepairPrimaryTokenRangeOnly(repairType));
                AutoRepairUtilsV2.updateStartAutoRepairHistory(repairType, myId, timeFunc.get(), turn);

                repairState.setRepairKeyspaceCount(0);
                repairState.setRepairTableSuccessCount(0);
                repairState.setRepairFailedTablesCount(0);
                repairState.setRepairSkippedTablesCount(0);
                repairState.setRepairInProgress(true);
                repairState.setTotalMVTablesConsideredForRepair(0);
                for (Keyspace keyspace : Keyspace.all()) {
                    Tables tables = keyspace.getMetadata().tables;
                    Iterator<TableMetadata> iter = tables.iterator();
                    String keyspaceName = keyspace.getName();
                    if (!AutoRepairUtilsV2.checkNodeContainsKeyspaceReplica(keyspace)) {
                        continue;
                    }

                    repairState.setRepairKeyspaceCount(repairState.getRepairKeyspaceCount() + 1);
                    List<String> tablesToBeRepaired = new ArrayList<>();
                    while (iter.hasNext()) {
                        repairState.setTotalTablesConsideredForRepair(repairState.getTotalTablesConsideredForRepair() + 1);
                        TableMetadata tableMetadata = iter.next();
                        String tableName = tableMetadata.name;
                        tablesToBeRepaired.add(tableName);

                        // See if we should repair MVs as well that are associated with this given table
                        List<String> mvs = AutoRepairUtilsV2.getAllMVs(repairType, keyspace, tableMetadata);
                        if (!mvs.isEmpty()) {
                            tablesToBeRepaired.addAll(mvs);
                            repairState.setTotalMVTablesConsideredForRepair(repairState.getTotalMVTablesConsideredForRepair() + mvs.size());
                        }
                    }

                    for (String tableName : tablesToBeRepaired) {
                        try {
                            ColumnFamilyStore columnFamilyStore = keyspace.getColumnFamilyStore(tableName);
                            if (keyspaceName.equals("ks")) {
                                logger.info("Repair is disabled for keyspace {} for tables: {}", keyspaceName, tableName);
                            }
                            if (columnFamilyStore.metadata().params.disableAutomatedRepair) {
                                logger.info("Repair is disabled for keyspace {} for tables: {}", keyspaceName, tableName);
                                repairState.setTotalDisabledTablesRepairCount(repairState.getTotalDisabledTablesRepairCount() + 1);
                                continue;
                            }
                            // this is done to make autorepair safe as running repair on table with more sstables
                            // may have its own challenges
                            int size = columnFamilyStore.getLiveSSTables().size();
                            if (size > config.getRepairSSTableCountHigherThreshold(repairType)) {
                                logger.info("Too many SSTables for repair, not doing repair on table {}.{} " +
                                        "totalSSTables {}", keyspaceName, tableName, columnFamilyStore.getLiveSSTables().size());
                                repairState.setRepairSkippedTablesCount(repairState.getRepairSkippedTablesCount() + 1);
                                continue;
                            }

                            if (config.getRepairByKeyspace(repairType)) {
                                logger.info("Repair keyspace {} for tables: {}", keyspaceName, tablesToBeRepaired);
                            } else {
                                logger.info("Repair table {}.{}", keyspaceName, tableName);
                            }
                            long tableStartTime = timeFunc.get();
                            boolean repairSuccess = true;
                            Set<Range<Token>> ranges = new HashSet<>();
                            List<Pair<Token, Token>> subRangesToBeRepaired = new AutoRepairTokenRangeDefault().getRange(repairType, primaryRangeOnly, keyspaceName, tableName);
                            int totalSubRanges = subRangesToBeRepaired.size();
                            int totalProcessedSubRanges = 0;
                            for (Pair<Token, Token> token : subRangesToBeRepaired) {
                                if (!config.isAutoRepairEnabled(repairType)) {
                                    logger.error("Auto-repair for type {} is disabled hence not running repair", repairType);
                                    repairState.setRepairInProgress(false);
                                    return;
                                }

                                if (config.getRepairByKeyspace(repairType)) {
                                    if (AutoRepairUtilsV2.keyspaceMaxRepairTimeExceeded(repairType, tableStartTime, tablesToBeRepaired.size())) {
                                        repairState.setRepairSkippedTablesCount(repairState.getRepairSkippedTablesCount() + tablesToBeRepaired.size());
                                        logger.info("Keyspace took too much time to repair hence skipping it {}",
                                                keyspaceName);
                                        break;
                                    }
                                } else {
                                    if (AutoRepairUtilsV2.tableMaxRepairTimeExceeded(repairType, tableStartTime)) {
                                        repairState.setRepairSkippedTablesCount(repairState.getRepairSkippedTablesCount() + 1);
                                        logger.info("Table took too much time to repair hence skipping it {}.{}",
                                                keyspaceName, tableName);
                                        break;
                                    }
                                }
                                Token childStartToken = token.left;
                                Token childEndToken = token.right;
                                logger.debug("Current Token Left side {}, right side {}", childStartToken
                                        .toString(), childEndToken.toString());

                                ranges.add(new Range<>(childStartToken, childEndToken));
                                totalProcessedSubRanges++;
                                if ((totalProcessedSubRanges % config.getRepairThreads(repairType) == 0) ||
                                        (totalProcessedSubRanges == totalSubRanges)) {
                                    RepairRunnable task = repairState.getRepairRunnable(keyspaceName,
                                            config.getRepairByKeyspace(repairType) ? tablesToBeRepaired : ImmutableList.of(tableName),
                                            ranges, primaryRangeOnly);
                                    new Thread(NamedThreadFactory.createAnonymousThread(new FutureTask<>(task, null))).start();
                                    try {
                                        repairState.waitForRepairToComplete();
                                    } catch (InterruptedException e) {
                                        logger.error("Exception in cond await:", e);
                                    }

                                    //check repair status
                                    if (repairState.isSuccess()) {
                                        logger.info("Repair completed for range {}-{} for {}.{}, total subranges: {}," +
                                                        "processed subranges: {}", childStartToken, childEndToken,
                                                keyspaceName, config.getRepairByKeyspace(repairType) ? tablesToBeRepaired : tableName, totalSubRanges, totalProcessedSubRanges);
                                    } else {
                                        repairSuccess = false;
                                        //in future we can add retry, etc.
                                        logger.info("Repair failed for range {}-{} for {}.{} total subranges: {}," +
                                                        "processed subranges: {}", childStartToken, childEndToken,
                                                keyspaceName, config.getRepairByKeyspace(repairType) ? tablesToBeRepaired : tableName, totalSubRanges, totalProcessedSubRanges);
                                    }
                                    ranges.clear();
                                }
                            }
                            int touchedTables = config.getRepairByKeyspace(repairType) ? tablesToBeRepaired.size() : 1;
                            if (repairSuccess) {
                                repairState.setRepairTableSuccessCount(repairState.getRepairTableSuccessCount() + touchedTables);
                            } else {
                                repairState.setRepairFailedTablesCount(repairState.getRepairFailedTablesCount() + touchedTables);
                            }
                            if (config.getRepairByKeyspace(repairType)) {
                                logger.info("Repair completed for keyspace {}, tables: {}", keyspaceName, tablesToBeRepaired);
                                break;
                            } else {
                                logger.info("Repair completed for {}.{}", keyspaceName, tableName);
                            }
                        } catch (Exception e) {
                            logger.error("Exception while repairing keyspace {}:", keyspaceName, e);
                        }
                    }
                }

                //if it was due to priority then remove it now
                if (turn == MY_TURN_DUE_TO_PRIORITY) {
                    logger.info("Remove current host from priority list");
                    AutoRepairUtilsV2.removePriorityStatus(repairType, myId);
                }

                repairState.setNodeRepairTimeInSec((int) TimeUnit.MILLISECONDS.toSeconds(timeFunc.get() - startTime));
                long timeInHours = TimeUnit.SECONDS.toHours(repairState.getNodeRepairTimeInSec());
                logger.info("Local {} repair time {} hour(s), stats: repairKeyspaceCount {}, " +
                                "repairTableSuccessCount {}, repairTableFailureCount {}, " +
                                "repairTableSkipCount {}", repairType, timeInHours, repairState.getRepairKeyspaceCount(),
                        repairState.getRepairTableSuccessCount(), repairState.getRepairFailedTablesCount(),
                        repairState.getRepairSkippedTablesCount());
                if (repairState.getLastRepairTime() != 0) {
                    repairState.setClusterRepairTimeInSec((int) TimeUnit.MILLISECONDS.toSeconds(timeFunc.get() -
                            repairState.getLastRepairTime()));
                    logger.info("Cluster repair time for repair type {}: {} day(s)", repairType,
                            TimeUnit.SECONDS.toDays(repairState.getClusterRepairTimeInSec()));
                }
                repairState.setLastRepairTime(timeFunc.get());
                if (timeInHours == 0 && millisToWait > 0) {
                    //If repair finished quickly, happens for an empty instance, in such case
                    //wait for a minute so that the JMX metrics can detect the repairInProgress
                    logger.info("Wait for {} milliseconds for repair type {}.", millisToWait, repairType);
                    Thread.sleep(millisToWait);
                }
                repairState.setRepairInProgress(false);
                AutoRepairUtilsV2.updateFinishAutoRepairHistory(repairType, myId, timeFunc.get());
            } else {
                logger.info("Waiting for my turn...");
            }
        } catch (Exception e) {
            logger.error("Exception in autorepair:", e);
        }
    }

    public AutoRepairState getRepairState(AutoRepairConfig.RepairType repairType) {
        return repairStates.get(repairType);
    }
}
