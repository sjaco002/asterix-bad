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
import java.util.Map.Entry;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveJob;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.ChannelJobInfo;
import org.apache.asterix.bad.runtime.RepetitiveChannelOperatorDescriptor;
import org.apache.asterix.external.feed.api.IActiveLifecycleEventSubscriber;
import org.apache.asterix.external.feed.api.IActiveLifecycleEventSubscriber.ActiveLifecycleEvent;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.runtime.util.AppContextInfo;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobInfo;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.log4j.Logger;

public class ChannelEventsListener implements IActiveEntityEventsListener {
    private static final Logger LOGGER = Logger.getLogger(ChannelEventsListener.class);
    private final List<IActiveLifecycleEventSubscriber> subscribers;
    private final Map<Long, ActiveJob> jobs;
    private final Map<EntityId, ChannelJobInfo> jobInfos;
    private EntityId entityId;

    public ChannelEventsListener(EntityId entityId) {
        this.entityId = entityId;
        subscribers = new ArrayList<>();
        jobs = new HashMap<>();
        jobInfos = new HashMap<>();
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

    private synchronized void handleJobStartEvent(ActiveEvent message) throws Exception {
        ActiveJob jobInfo = jobs.get(message.getJobId().getId());
        handleJobStartMessage((ChannelJobInfo) jobInfo);
    }

    private synchronized void handleJobFinishEvent(ActiveEvent message) throws Exception {
        ActiveJob jobInfo = jobs.get(message.getJobId().getId());
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Channel Job finished for  " + jobInfo);
        }
        handleJobFinishMessage((ChannelJobInfo) jobInfo);
    }

    private synchronized void handleJobFinishMessage(ChannelJobInfo cInfo) throws Exception {
        EntityId channelJobId = cInfo.getEntityId();

        IHyracksClientConnection hcc = AppContextInfo.INSTANCE.getHcc();
        JobInfo info = hcc.getJobInfo(cInfo.getJobId());
        JobStatus status = info.getStatus();
        boolean failure = status != null && status.equals(JobStatus.FAILURE);

        jobInfos.remove(channelJobId);
        jobs.remove(cInfo.getJobId().getId());
        // notify event listeners
        ActiveLifecycleEvent event = failure ? ActiveLifecycleEvent.ACTIVE_JOB_FAILED
                : ActiveLifecycleEvent.ACTIVE_JOB_ENDED;
        notifyEventSubscribers(event);
    }

    private void notifyEventSubscribers(ActiveLifecycleEvent event) {
        if (subscribers != null && !subscribers.isEmpty()) {
            for (IActiveLifecycleEventSubscriber subscriber : subscribers) {
                subscriber.handleEvent(event);
            }
        }
    }

    private static synchronized void handleJobStartMessage(ChannelJobInfo cInfo) throws Exception {
        List<OperatorDescriptorId> channelOperatorIds = new ArrayList<>();
        Map<OperatorDescriptorId, IOperatorDescriptor> operators = cInfo.getSpec().getOperatorMap();
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            if (opDesc instanceof RepetitiveChannelOperatorDescriptor) {
                channelOperatorIds.add(opDesc.getOperatorId());
            }
        }

        IHyracksClientConnection hcc = AppContextInfo.INSTANCE.getHcc();
        JobInfo info = hcc.getJobInfo(cInfo.getJobId());
        List<String> locations = new ArrayList<>();
        for (OperatorDescriptorId channelOperatorId : channelOperatorIds) {
            Map<Integer, String> operatorLocations = info.getOperatorLocations().get(channelOperatorId);
            int nOperatorInstances = operatorLocations.size();
            for (int i = 0; i < nOperatorInstances; i++) {
                locations.add(operatorLocations.get(i));
            }
        }
        cInfo.setLocations(locations);
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

        ChannelJobInfo cInfo = new ChannelJobInfo(entityId, jobId, ActivityState.CREATED, jobSpec);
        jobs.put(jobId.getId(), cInfo);
        jobInfos.put(entityId, cInfo);

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Registered channel job [" + jobId + "]" + " for channel " + entityId);
        }

        notifyEventSubscribers(ActiveLifecycleEvent.ACTIVE_JOB_STARTED);

    }

    public JobSpecification getJobSpecification(EntityId activeJobId) {
        return jobInfos.get(activeJobId).getSpec();
    }

    public ChannelJobInfo getJobInfo(EntityId activeJobId) {
        return jobInfos.get(activeJobId);
    }

    public synchronized void registerEventSubscriber(IActiveLifecycleEventSubscriber subscriber) {
        subscribers.add(subscriber);
    }

    public void deregisterEventSubscriber(IActiveLifecycleEventSubscriber subscriber) {
        subscribers.remove(subscriber);
    }

    public synchronized boolean isChannelActive(EntityId activeJobId, IActiveLifecycleEventSubscriber eventSubscriber) {
        boolean active = false;
        ChannelJobInfo cInfo = jobInfos.get(activeJobId);
        if (cInfo != null) {
            active = cInfo.getState().equals(ActivityState.ACTIVE);
        }
        if (active) {
            registerEventSubscriber(eventSubscriber);
        }
        return active;
    }

    public FeedConnectionId[] getConnections() {
        return jobInfos.keySet().toArray(new FeedConnectionId[jobInfos.size()]);
    }

    @Override
    public boolean isEntityActive() {
        return !jobs.isEmpty();
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
