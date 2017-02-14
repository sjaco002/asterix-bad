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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveJob;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.DistributedJobInfo;
import org.apache.asterix.external.feed.api.IActiveLifecycleEventSubscriber;
import org.apache.asterix.external.feed.api.IActiveLifecycleEventSubscriber.ActiveLifecycleEvent;
import org.apache.asterix.runtime.utils.AppContextInfo;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobInfo;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.log4j.Logger;

public class PrecompiledJobEventListener implements IActiveEntityEventsListener {
    private static final Logger LOGGER = Logger.getLogger(PrecompiledJobEventListener.class);
    private final List<IActiveLifecycleEventSubscriber> subscribers;
    private final Map<Long, ActiveJob> jobs;
    private final Map<EntityId, DistributedJobInfo> jobInfos;
    private EntityId entityId;
    private JobId hyracksJobId;
    private ScheduledExecutorService executorService = null;
    private boolean active;
    private ResultReader resultReader;
    private ResultSetId resultSetId;

    public enum PrecompiledType {
        CHANNEL,
        QUERY,
        INSERT,
        DELETE
    }

    private final PrecompiledType type;

    public PrecompiledJobEventListener(EntityId entityId, PrecompiledType type) {
        this.entityId = entityId;
        subscribers = new ArrayList<>();
        jobs = new HashMap<>();
        jobInfos = new HashMap<>();
        active = false;
        this.type = type;
    }

    public ResultReader getResultReader() {
        return resultReader;
    }

    public ResultSetId getResultSetId() {
        return resultSetId;
    }

    public PrecompiledType getType() {
        return type;
    }

    @Override
    public void notify(ActiveEvent event) {
        try {
            switch (event.getEventKind()) {
                case JOB_START:
                    handleJobStartEvent(event);
                    break;
                case JOB_FINISH:
                    handleJobFinishEvent(event);
                    break;
                case PARTITION_EVENT:
                    LOGGER.warn("Partition Channel Event");
                    break;
                default:
                    break;

            }
        } catch (Exception e) {
            LOGGER.error("Unhandled Exception", e);
        }
    }

    public void storeDistributedInfo(JobId jobId, ScheduledExecutorService ses, ResultReader resultReader,
            ResultSetId resultSetId) {
        this.hyracksJobId = jobId;
        this.executorService = ses;
        this.resultReader = resultReader;
        this.resultSetId = resultSetId;
    }

    public ScheduledExecutorService getExecutorService() {
        return executorService;
    }

    public JobId getHyracksJobId() {
        return hyracksJobId;
    }

    private synchronized void handleJobStartEvent(ActiveEvent message) throws Exception {
        ActiveJob jobInfo = jobs.get(message.getJobId().getId());
        handleJobStartMessage((DistributedJobInfo) jobInfo);
        active = true;
    }

    private synchronized void handleJobFinishEvent(ActiveEvent message) throws Exception {
        ActiveJob jobInfo = jobs.get(message.getJobId().getId());
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Channel Job finished for  " + jobInfo);
        }
        handleJobFinishMessage((DistributedJobInfo) jobInfo);
    }

    private synchronized void handleJobFinishMessage(DistributedJobInfo cInfo) throws Exception {
        if (!isEntityActive()) {
            EntityId channelJobId = cInfo.getEntityId();

            IHyracksClientConnection hcc = AppContextInfo.INSTANCE.getHcc();
            JobInfo info = hcc.getJobInfo(cInfo.getJobId());
            JobStatus status = info.getStatus();
            boolean failure = status != null && status.equals(JobStatus.FAILURE);

            jobInfos.remove(channelJobId);
            jobs.remove(cInfo.getJobId().getId());
            // notify event listeners
            ActiveLifecycleEvent event =
                    failure ? ActiveLifecycleEvent.ACTIVE_JOB_FAILED : ActiveLifecycleEvent.ACTIVE_JOB_ENDED;
            notifyEventSubscribers(event);
        }
    }

    private void notifyEventSubscribers(ActiveLifecycleEvent event) {
        if (subscribers != null && !subscribers.isEmpty()) {
            for (IActiveLifecycleEventSubscriber subscriber : subscribers) {
                subscriber.handleEvent(event);
            }
        }
    }

    private static synchronized void handleJobStartMessage(DistributedJobInfo cInfo) throws Exception {
        cInfo.setState(ActivityState.ACTIVE);
    }

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification spec) {
        try {
            registerJob(jobId, spec);
            return;

        } catch (Exception e) {
            LOGGER.error(e);
        }
    }

    public synchronized void registerJob(JobId jobId, JobSpecification jobSpec) {
        if (jobs.get(jobId.getId()) != null) {
            throw new IllegalStateException("Channel job already registered");
        }
        if (jobInfos.containsKey(jobId.getId())) {
            throw new IllegalStateException("Channel job already registered");
        }

        DistributedJobInfo cInfo = new DistributedJobInfo(entityId, jobId, ActivityState.CREATED, jobSpec);
        jobs.put(jobId.getId(), cInfo);
        jobInfos.put(entityId, cInfo);

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Registered channel job [" + jobId + "]" + " for channel " + entityId);
        }

        notifyEventSubscribers(ActiveLifecycleEvent.ACTIVE_JOB_STARTED);

    }

    @Override
    public boolean isEntityActive() {
        return active;
    }

    public void deActivate() {
        active = false;
    }

    @Override
    public EntityId getEntityId() {
        return entityId;
    }

    @Override
    public boolean isEntityUsingDataset(String dataverseName, String datasetName) {
        if (entityId.getDataverse().equals(dataverseName)) {
            String subscriptionsName = entityId.getEntityName() + BADConstants.subscriptionEnding;
            String resultsName = entityId.getEntityName() + BADConstants.resultsEnding;
            if (datasetName.equals(subscriptionsName) || datasetName.equals(resultsName)) {
                return true;
            }
        }
        return false;
    }
}
