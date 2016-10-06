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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActiveSourceOperatorNodePushable;
import org.apache.asterix.bad.ChannelJobService;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;

public class RepetitiveChannelOperatorNodePushable extends ActiveSourceOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(RepetitiveChannelOperatorNodePushable.class.getName());

    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private final JobSpecification jobSpec;
    private long duration;
    private ChannelJobService channelJobService;

    public RepetitiveChannelOperatorNodePushable(IHyracksTaskContext ctx, ActiveRuntimeId runtimeId,
            JobSpecification channeljobSpec, String duration, String strIP, int port) throws Exception {
        super(ctx, runtimeId);
        this.jobSpec = channeljobSpec;
        this.duration = findPeriod(duration);
        //TODO: we should share channelJobService as a single instance
        //And only create one hcc
        channelJobService = new ChannelJobService(strIP, port);
    }

    public void executeJob() throws Exception {
        LOGGER.info("Executing Job: " + runtimeId.toString());
        channelJobService.runChannelJob(jobSpec);
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
            throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    protected void start() throws HyracksDataException, InterruptedException {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    executeJob();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, duration, duration, TimeUnit.MILLISECONDS);

        while (!scheduledExecutorService.isTerminated()) {

        }

    }

    @Override
    protected void abort() throws HyracksDataException, InterruptedException {
        scheduledExecutorService.shutdown();
    }

    private long findPeriod(String duration) {
        //TODO: Allow Repetitive Channels to use YMD durations  
        String hoursMinutesSeconds = "";
        if (duration.indexOf('T') != -1) {
            hoursMinutesSeconds = duration.substring(duration.indexOf('T') + 1);
        }
        double seconds = 0;
        if (hoursMinutesSeconds != "") {
            int pos = 0;
            if (hoursMinutesSeconds.indexOf('H') != -1) {
                Double hours = Double.parseDouble(hoursMinutesSeconds.substring(pos, hoursMinutesSeconds.indexOf('H')));
                seconds += (hours * 60 * 60);
                pos = hoursMinutesSeconds.indexOf('H') + 1;

            }
            if (hoursMinutesSeconds.indexOf('M') != -1) {
                Double minutes = Double
                        .parseDouble(hoursMinutesSeconds.substring(pos, hoursMinutesSeconds.indexOf('M')));
                seconds += (minutes * 60);
                pos = hoursMinutesSeconds.indexOf('M') + 1;
            }
            if (hoursMinutesSeconds.indexOf('S') != -1) {
                Double s = Double.parseDouble(hoursMinutesSeconds.substring(pos, hoursMinutesSeconds.indexOf('S')));
                seconds += (s);
            }

        }
        return (long) (seconds * 1000);
    }

}
