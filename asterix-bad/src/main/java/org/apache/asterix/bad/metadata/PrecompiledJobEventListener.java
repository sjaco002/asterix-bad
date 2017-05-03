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

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEventSubscriber;
import org.apache.asterix.common.metadata.IDataset;
import org.apache.asterix.external.feed.management.ActiveEntityEventsListener;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.log4j.Logger;

public class PrecompiledJobEventListener extends ActiveEntityEventsListener {
    private static final Logger LOGGER = Logger.getLogger(PrecompiledJobEventListener.class);

    private ScheduledExecutorService executorService = null;
    private IHyracksDataset hdc;
    private ResultSetId resultSetId;

    public enum PrecompiledType {
        CHANNEL,
        QUERY,
        INSERT,
        DELETE
    }

    private final PrecompiledType type;

    public PrecompiledJobEventListener(EntityId entityId, PrecompiledType type, List<IDataset> datasets) {
        this.entityId = entityId;
        this.datasets = datasets;
        state = ActivityState.STOPPED;
        this.type = type;
    }

    public IHyracksDataset getResultDataset() {
        return hdc;
    }

    public ResultSetId getResultId() {
        return resultSetId;
    }

    public PrecompiledType getType() {
        return type;
    }

    public void storeDistributedInfo(JobId jobId, ScheduledExecutorService ses,
            IHyracksDataset hdc, ResultSetId resultSetId) {
        this.jobId = jobId;
        this.executorService = ses;
        this.hdc = hdc;
        this.resultSetId = resultSetId;
    }

    public ScheduledExecutorService getExecutorService() {
        return executorService;
    }

    public boolean isEntityActive() {
        return state == ActivityState.STARTED;
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

    private synchronized void handleJobStartEvent(ActiveEvent message) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Channel Job started for  " + entityId);
        }
        state = ActivityState.STARTED;
    }

    private synchronized void handleJobFinishEvent(ActiveEvent message) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Channel Job finished for  " + entityId);
        }
    }

    @Override
    public IActiveEventSubscriber subscribe(ActivityState state) throws HyracksDataException {
        return null;
    }
}
