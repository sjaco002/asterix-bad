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
package org.apache.asterix.bad;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.EnumSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AUUID;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

/**
 * Provides functionality for running channel jobs and communicating with Brokers
 */
public class ChannelJobService {

    private static final Logger LOGGER = Logger.getLogger(ChannelJobService.class.getName());

    public static ScheduledExecutorService startJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags, JobId jobId,
            IHyracksClientConnection hcc, long duration)
            throws Exception {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    executeJob(jobSpec, jobFlags, jobId, hcc);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Channel Job Failed to run.", e);
                }
            }
        }, duration, duration, TimeUnit.MILLISECONDS);
        return scheduledExecutorService;
    }

    public static void executeJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags, JobId jobId,
            IHyracksClientConnection hcc)
            throws Exception {
        LOGGER.info("Executing Channel Job");
        if (jobId == null) {
            hcc.startJob(jobSpec, jobFlags);
        } else {
            hcc.startJob(jobSpec, jobFlags, jobId);
        }
    }

    public static void runChannelJob(JobSpecification channeljobSpec, IHyracksClientConnection hcc) throws Exception {
        JobId jobId = hcc.startJob(channeljobSpec);
        hcc.waitForCompletion(jobId);
    }

    public static void sendBrokerNotificationsForChannel(EntityId activeJobId, String brokerEndpoint,
            AOrderedList subscriptionIds, String channelExecutionTime) throws HyracksDataException {
        String formattedString;
            formattedString = formatJSON(activeJobId, subscriptionIds, channelExecutionTime);
        sendMessage(brokerEndpoint, formattedString);
    }

    public static String formatJSON(EntityId activeJobId, AOrderedList subscriptionIds, String channelExecutionTime) {
        String JSON = "{ \"dataverseName\":\"" + activeJobId.getDataverse() + "\", \"channelName\":\""
                + activeJobId.getEntityName() + "\", \"" + BADConstants.ChannelExecutionTime + "\":\""
                + channelExecutionTime + "\", \"subscriptionIds\":[";
        for (int i = 0; i < subscriptionIds.size(); i++) {
            AUUID subId = (AUUID) subscriptionIds.getItem(i);
            String subString = subId.toSimpleString();
            JSON += "\"" + subString + "\"";
            if (i < subscriptionIds.size() - 1) {
                JSON += ",";
            }
        }
        JSON += "]}";
        return JSON;

    }

    public static long findPeriod(String duration) {
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
                Double minutes =
                        Double.parseDouble(hoursMinutesSeconds.substring(pos, hoursMinutesSeconds.indexOf('M')));
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

    public static void sendMessage(String targetURL, String urlParameters) {
        HttpURLConnection connection = null;
        try {
            //Create connection
            URL url = new URL(targetURL);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            connection.setRequestProperty("Content-Length", Integer.toString(urlParameters.getBytes().length));
            connection.setRequestProperty("Content-Language", "en-US");

            connection.setUseCaches(false);
            connection.setDoOutput(true);

            if (connection.getOutputStream() != null) {
                //Send message
                DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
                wr.writeBytes(urlParameters);
                wr.close();
            } else {
                LOGGER.log(Level.WARNING, "Channel Failed to connect to Broker.");
            }

            if (LOGGER.isLoggable(Level.INFO)) {
                int responseCode = connection.getResponseCode();
                LOGGER.info("\nSending 'POST' request to URL : " + url);
                LOGGER.info("Post parameters : " + urlParameters);
                LOGGER.info("Response Code : " + responseCode);
            }

            if (connection.getInputStream() != null) {
                BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.log(Level.INFO, response.toString());
                }
            } else {
                LOGGER.log(Level.WARNING, "Channel Failed to get response from Broker.");
            }

        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Channel Failed to connect to Broker.");
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    @Override
    public String toString() {
        return "ChannelJobService";
    }

}
