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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AUUID;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public class NotifyBrokerRuntime extends AbstractOneInputOneOutputOneFramePushRuntime {
    private static final Logger LOGGER = Logger.getLogger(NotifyBrokerRuntime.class.getName());

    private final ByteBufferInputStream bbis = new ByteBufferInputStream();
    private final DataInputStream di = new DataInputStream(bbis);
    private final AOrderedListSerializerDeserializer subSerDes =
            new AOrderedListSerializerDeserializer(new AOrderedListType(BuiltinType.AUUID, null));
    private final ARecordSerializerDeserializer recordSerDes;

    private IPointable inputArg0 = new VoidPointable();
    private IPointable inputArg1 = new VoidPointable();
    private IPointable inputArg2 = new VoidPointable();
    private IScalarEvaluator eval0;
    private IScalarEvaluator eval1;
    private IScalarEvaluator eval2;
    private final ActiveManager activeManager;
    private final EntityId entityId;
    private final boolean push;
    private AOrderedList pushList;
    private ARecord pushRecord;
    private final IAType recordType;
    private final Map<String, HashSet<String>> sendData = new HashMap<>();
    private String executionTimeString;
    String endpoint;

    public NotifyBrokerRuntime(IHyracksTaskContext ctx, IScalarEvaluatorFactory brokerEvalFactory,
            IScalarEvaluatorFactory pushListEvalFactory, IScalarEvaluatorFactory channelExecutionEvalFactory,
            EntityId activeJobId, boolean push, IAType recordType) throws HyracksDataException {
        this.tRef = new FrameTupleReference();
        eval0 = brokerEvalFactory.createScalarEvaluator(ctx);
        eval1 = pushListEvalFactory.createScalarEvaluator(ctx);
        eval2 = channelExecutionEvalFactory.createScalarEvaluator(ctx);
        this.activeManager = (ActiveManager) ((INcApplicationContext) ctx.getJobletContext().getServiceContext()
                .getApplicationContext()).getActiveManager();
        this.entityId = activeJobId;
        this.push = push;
        this.pushList = null;
        this.pushRecord = null;
        this.recordType = recordType;
        recordSerDes = new ARecordSerializerDeserializer((ARecordType) recordType);
        executionTimeString = null;
    }

    @Override
    public void open() throws HyracksDataException {
        return;
    }

    private void addSubscriptions(String endpoint, AOrderedList subscriptionIds) {
        for (int i = 0; i < subscriptionIds.size(); i++) {
            AUUID subId = (AUUID) subscriptionIds.getItem(i);
            String subscriptionString = subId.toString();
            //Broker code currently cannot handle the "uuid {}" part of the string, so we parse just the value
            subscriptionString = subscriptionString.substring(8, subscriptionString.length() - 2);
            subscriptionString = "\"" + subscriptionString + "\"";
            sendData.get(endpoint).add(subscriptionString);
        }
    }

    public String createData(String endpoint) {
        String JSON = "{ \"dataverseName\":\"" + entityId.getDataverse() + "\", \"channelName\":\""
                + entityId.getEntityName() + "\", \"" + BADConstants.ChannelExecutionTime + "\":\""
                + executionTimeString + "\", \"subscriptionIds\":[";
        for (String value : sendData.get(endpoint)) {
            JSON += value;
            JSON += ",";
        }
        JSON = JSON.substring(0, JSON.length() - 1);
        JSON += "]}";
        return JSON;

    }

    private void sendGroupOfResults(String endpoint) {
        String urlParameters = createData(endpoint);
        try {
            //Create connection
            URL url = new URL(endpoint);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            connection.setRequestProperty("Content-Length", Integer.toString(urlParameters.getBytes().length));
            connection.setRequestProperty("Content-Language", "en-US");

            connection.setUseCaches(false);
            connection.setDoOutput(true);
            connection.setConnectTimeout(500);
            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            wr.writeBytes(urlParameters);
            if (LOGGER.isLoggable(Level.INFO)) {
                int responseCode = connection.getResponseCode();
                LOGGER.info("\nSending 'POST' request to URL : " + url);
                LOGGER.info("Post parameters : " + urlParameters);
                LOGGER.info("Response Code : " + responseCode);
            }
            wr.close();
            connection.disconnect();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Channel Failed to connect to Broker.");
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tAccess.reset(buffer);
        int nTuple = tAccess.getTupleCount();
        for (int t = 0; t < nTuple; t++) {
            tRef.reset(tAccess, t);

            eval0.evaluate(tRef, inputArg0);
            eval1.evaluate(tRef, inputArg1);
            eval2.evaluate(tRef, inputArg2);

            if (executionTimeString == null) {
                int resultSetOffset = inputArg2.getStartOffset();
                bbis.setByteBuffer(tRef.getFrameTupleAccessor().getBuffer(), resultSetOffset + 1);
                ADateTime executionTime = ADateTimeSerializerDeserializer.INSTANCE.deserialize(di);
                try {
                    executionTimeString = executionTime.toSimpleString();
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }

            int serBrokerOffset = inputArg0.getStartOffset();
            bbis.setByteBuffer(tRef.getFrameTupleAccessor().getBuffer(), serBrokerOffset + 1);
            endpoint = AStringSerializerDeserializer.INSTANCE.deserialize(di).getStringValue();
            sendData.putIfAbsent(endpoint, new HashSet<>());

            if (push) {
                int pushOffset = inputArg1.getStartOffset();
                bbis.setByteBuffer(tRef.getFrameTupleAccessor().getBuffer(), pushOffset + 1);
                //TODO: Right now this creates an object per channel result. Need to find a better way to deserialize
                pushRecord = recordSerDes.deserialize(di);
                sendData.get(endpoint).add(pushRecord.toString());

            } else {
                int serSubOffset = inputArg1.getStartOffset();
                bbis.setByteBuffer(tRef.getFrameTupleAccessor().getBuffer(), serSubOffset + 1);
                pushList = subSerDes.deserialize(di);
                addSubscriptions(endpoint, pushList);
            }
        }

    }

    @Override
    public void close() throws HyracksDataException {
        for (String endpoint : sendData.keySet()) {
            if (sendData.get(endpoint).size() > 0) {
                sendGroupOfResults(endpoint);
                sendData.get(endpoint).clear();
            }
        }
        return;
    }

    @Override
    public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
        this.inputRecordDesc = recordDescriptor;
        this.tAccess = new FrameTupleAccessor(inputRecordDesc);
    }

    @Override
    public void flush() throws HyracksDataException {
        return;
    }

    @Override
    public void fail() throws HyracksDataException {
        failed = true;
    }
}
