/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.bad.metadata;

import java.util.concurrent.ScheduledExecutorService;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventSubscriber;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.metadata.IDataset;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.log4j.Logger;

public class DeployedJobSpecEventListener implements IActiveEntityEventsListener {

    private static final Logger LOGGER = Logger.getLogger(DeployedJobSpecEventListener.class);


    public enum PrecompiledType {
        CHANNEL,
        PUSH_CHANNEL,
        QUERY,
        INSERT,
        DELETE
    }

    public enum InstanceChange {
        LOCK,
        INCREASE,
        DECREASE,
        UNLOCK
    }

    private DeployedJobSpecId deployedJobSpecId;
    private ScheduledExecutorService executorService = null;
    private final PrecompiledType type;

    private IHyracksDataset hdc;
    private ResultSetId resultSetId;
    private int instanceCount;

    // members
    protected volatile ActivityState state;
    protected final ICcApplicationContext appCtx;
    protected final EntityId entityId;
    protected final ActiveEvent statsUpdatedEvent;
    protected long statsTimestamp;
    protected String stats;
    protected final String runtimeName;
    protected final AlgebricksAbsolutePartitionConstraint locations;
    private int runningInstance;

    public DeployedJobSpecEventListener(ICcApplicationContext appCtx, EntityId entityId, PrecompiledType type,
            AlgebricksAbsolutePartitionConstraint locations, String runtimeName) {
        this.appCtx = appCtx;
        this.entityId = entityId;
        this.state = ActivityState.STOPPED;
        this.statsTimestamp = -1;
        this.instanceCount = 0;
        this.statsUpdatedEvent = new ActiveEvent(null, Kind.STATS_UPDATED, entityId, null);
        this.stats = "{\"Stats\":\"N/A\"}";
        this.runtimeName = runtimeName;
        this.locations = locations;
        state = ActivityState.STOPPED;
        this.type = type;
    }


    public IHyracksDataset getResultDataset() {
        return hdc;
    }

    public ResultSetId getResultId() {
        return resultSetId;
    }

    public DeployedJobSpecId getDeployedJobSpecId() {
        return deployedJobSpecId;
    }

    @Override
    public EntityId getEntityId() {
        return entityId;
    }

    @Override
    public ActivityState getState() {
        return state;
    }

    @Override
    public boolean isEntityUsingDataset(IDataset dataset) {
        return false;
    }

    @Override
    public String getStats() {
        return stats;
    }

    @Override
    public long getStatsTimeStamp() {
        return statsTimestamp;
    }

    public synchronized boolean setInstanceCount(InstanceChange change) {
        switch (change) {
            case INCREASE:
                if (instanceCount < 0) {
                    return false;
                }
                instanceCount++;
                break;
            case DECREASE:
                instanceCount--;
                break;
            case LOCK:
                if (instanceCount != 0) {
                    return false;
                }
                instanceCount = -1;
                break;
            case UNLOCK:
                instanceCount = 0;
                break;
        }
        return true;
    }

    public PrecompiledType getType() {
        return type;
    }

    public void storeDistributedInfo(DeployedJobSpecId deployedJobSpecId, ScheduledExecutorService ses,
            IHyracksDataset hdc, ResultSetId resultSetId) {
        this.deployedJobSpecId = deployedJobSpecId;
        this.executorService = ses;
        this.hdc = hdc;
        this.resultSetId = resultSetId;
    }

    public ScheduledExecutorService getExecutorService() {
        return executorService;
    }

    public void deActivate() {
        state = ActivityState.STOPPED;
    }

    @Override
    public void notify(ActiveEvent event) {
        try {
            switch (event.getEventKind()) {
                case JOB_STARTED:
                    handleJobStartEvent(event);
                    break;
                case JOB_FINISHED:
                    handleJobFinishEvent(event);
                    break;
                default:
                    break;

            }
        } catch (Exception e) {
            LOGGER.error("Unhandled Exception", e);
        }
    }

    @Override
    public void refreshStats(long l) throws HyracksDataException {
        // no op
    }

    private synchronized void handleJobStartEvent(ActiveEvent message) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Channel Job started for  " + entityId);
        }
        runningInstance++;
        state = ActivityState.RUNNING;
    }

    private synchronized void handleJobFinishEvent(ActiveEvent message) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Channel Job finished for  " + entityId);
        }
        runningInstance--;
        if (runningInstance == 0) {
            state = ActivityState.STOPPED;
        }
    }

    @Override
    public synchronized void subscribe(IActiveEntityEventSubscriber subscriber) throws HyracksDataException {
        // no op
    }

    @Override
    public boolean isActive() {
        return state == ActivityState.RUNNING;
    }

    @Override
    public void unregister() throws HyracksDataException {
    }

    @Override
    public Exception getJobFailure() {
        return null;
    }

    @Override
    public String getDisplayName() throws HyracksDataException {
        return this.entityId.toString();
    }

    public int getRunningInstance() {
        return runningInstance;
    }
}
