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
package org.apache.asterix.bad.runtime;

import java.util.EnumSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActiveSourceOperatorNodePushable;
import org.apache.asterix.bad.ChannelJobService;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobSpecification;

public class RepetitiveChannelOperatorNodePushable extends ActiveSourceOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(RepetitiveChannelOperatorNodePushable.class.getName());

    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private final JobSpecification jobSpec;
    private long duration;
    private final HyracksConnection hcc;

    public RepetitiveChannelOperatorNodePushable(IHyracksTaskContext ctx, ActiveRuntimeId runtimeId,
            JobSpecification channeljobSpec, String duration, String strIP, int port) throws HyracksDataException {
        super(ctx, runtimeId);
        this.jobSpec = channeljobSpec;
        this.duration = ChannelJobService.findPeriod(duration);
        try {
            hcc = new HyracksConnection(strIP, port);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }


    @Override
    protected void start() throws HyracksDataException, InterruptedException {
        try {
            scheduledExecutorService =
                    ChannelJobService.startJob(jobSpec, EnumSet.noneOf(JobFlag.class), null, hcc, duration);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        while (!scheduledExecutorService.isTerminated()) {

        }

    }

    @Override
    protected void abort() throws HyracksDataException, InterruptedException {
        scheduledExecutorService.shutdown();
    }

}
