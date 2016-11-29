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
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.json.JSONException;

/**
 * Provides functionality for running channel jobs and communicating with Brokers
 */
public class ChannelJobService {

    private static final Logger LOGGER = Logger.getLogger(ChannelJobService.class.getName());
    IHyracksClientConnection hcc;

    public ChannelJobService(String strIP, int port) throws Exception {
        if (port != -1) {
            hcc = new HyracksConnection(strIP, port);
        }
    }

    public ChannelJobService(IHyracksClientConnection hcc) {
        this.hcc = hcc;
    }

    public void startJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags, JobId jobId) throws Exception {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    executeJob(jobSpec, jobFlags, jobId);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 10000, 10000, TimeUnit.MILLISECONDS);
        //TODO: Use actual channel interval
    }

    public void executeJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags, JobId jobId) throws Exception {
        hcc.startJob(jobSpec, jobFlags, jobId);
    }

    public void runChannelJob(JobSpecification channeljobSpec) throws Exception {
        JobId jobId = hcc.startJob(channeljobSpec);
        hcc.waitForCompletion(jobId);
    }

    public void sendBrokerNotificationsForChannel(EntityId activeJobId, String brokerEndpoint,
            AOrderedList subscriptionIds, String channelExecutionTime) throws HyracksDataException {
        String formattedString;
        try {
            formattedString = formatJSON(activeJobId, subscriptionIds, channelExecutionTime);
        } catch (JSONException e) {
            throw new HyracksDataException(e);
        }
        sendMessage(brokerEndpoint, formattedString);
    }

    public String formatJSON(EntityId activeJobId, AOrderedList subscriptionIds, String channelExecutionTime)
            throws JSONException {
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
                try {
                    DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
                    wr.writeBytes(urlParameters);
                    wr.close();
                } catch (Exception e) {
                    throw e;
                }
            } else {
                throw new Exception();
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
                    System.out.println(response.toString());
                }
            } else {
                throw new Exception();
            }

        } catch (Exception e) {
            LOGGER.info("Broker connection failed to write");
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
