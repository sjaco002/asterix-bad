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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventSubscriber;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.metadata.IDataset;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.PreDistributedId;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class PrecompiledJobEventListener implements IActiveEntityEventsListener {

    private static final Logger LOGGER = Logger.getLogger(PrecompiledJobEventListener.class);


    public enum PrecompiledType {
        CHANNEL,
        QUERY,
        INSERT,
        DELETE
    }

    enum RequestState {
        INIT,
        STARTED,
        FINISHED
    }

    private PreDistributedId preDistributedId;
    private ScheduledExecutorService executorService = null;
    private ResultReader resultReader;
    private final PrecompiledType type;

    private IHyracksDataset hdc;
    private ResultSetId resultSetId;
    // members
    protected volatile ActivityState state;
    protected JobId jobId;
    protected final List<IActiveEntityEventSubscriber> subscribers = new ArrayList<>();
    protected final ICcApplicationContext appCtx;
    protected final EntityId entityId;
    protected final List<IDataset> datasets;
    protected final ActiveEvent statsUpdatedEvent;
    protected long statsTimestamp;
    protected String stats;
    protected RequestState statsRequestState;
    protected final String runtimeName;
    protected final AlgebricksAbsolutePartitionConstraint locations;
    protected int numRegistered;

    public PrecompiledJobEventListener(ICcApplicationContext appCtx, EntityId entityId, PrecompiledType type,
            List<IDataset> datasets, AlgebricksAbsolutePartitionConstraint locations, String runtimeName) {
        this.appCtx = appCtx;
        this.entityId = entityId;
        this.datasets = datasets;
        this.state = ActivityState.STOPPED;
        this.statsTimestamp = -1;
        this.statsRequestState = RequestState.INIT;
        this.statsUpdatedEvent = new ActiveEvent(null, Kind.STATS_UPDATED, entityId, null);
        this.stats = "{\"Stats\":\"N/A\"}";
        this.runtimeName = runtimeName;
        this.locations = locations;
        this.numRegistered = 0;
        state = ActivityState.STOPPED;
        this.type = type;
    }


    public IHyracksDataset getResultDataset() {
        return hdc;
    }

    public ResultSetId getResultId() {
        return resultSetId;
    }

    public PreDistributedId getPredistributedId() {
        return preDistributedId;
    }

    protected synchronized void handle(ActivePartitionMessage message) {
        if (message.getEvent() == ActivePartitionMessage.Event.RUNTIME_REGISTERED) {
            numRegistered++;
            if (numRegistered == locations.getLocations().length) {
                state = ActivityState.RUNNING;
            }
        }
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
        return datasets.contains(dataset);
    }

    public JobId getJobId() {
        return jobId;
    }

    @Override
    public String getStats() {
        return stats;
    }

    @Override
    public long getStatsTimeStamp() {
        return statsTimestamp;
    }

    public String formatStats(List<String> responses) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("{\"Stats\": [").append(responses.get(0));
        for (int i = 1; i < responses.size(); i++) {
            strBuilder.append(", ").append(responses.get(i));
        }
        strBuilder.append("]}");
        return strBuilder.toString();
    }

    protected synchronized void notifySubscribers(ActiveEvent event) {
        notifyAll();
        Iterator<IActiveEntityEventSubscriber> it = subscribers.iterator();
        while (it.hasNext()) {
            IActiveEntityEventSubscriber subscriber = it.next();
            if (subscriber.isDone()) {
                it.remove();
            } else {
                try {
                    subscriber.notify(event);
                } catch (HyracksDataException e) {
                    LOGGER.log(Level.WARN, "Failed to notify subscriber", e);
                }
                if (subscriber.isDone()) {
                    it.remove();
                }
            }
        }
    }

    public AlgebricksAbsolutePartitionConstraint getLocations() {
        return locations;
    }

    public ResultReader getResultReader() {
        return resultReader;
    }

    public PrecompiledType getType() {
        return type;
    }

    public void storeDistributedInfo(PreDistributedId preDistributedId, ScheduledExecutorService ses,
            IHyracksDataset hdc, ResultSetId resultSetId) {
        this.preDistributedId = preDistributedId;
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
        state = ActivityState.RUNNING;
    }

    private synchronized void handleJobFinishEvent(ActiveEvent message) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Channel Job finished for  " + entityId);
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
}
