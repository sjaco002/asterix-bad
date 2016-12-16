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
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.bad.ChannelJobService;
import org.apache.asterix.common.api.IAppRuntimeContext;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
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

    private final ByteBufferInputStream bbis = new ByteBufferInputStream();
    private final DataInputStream di = new DataInputStream(bbis);
    private final AOrderedListSerializerDeserializer subSerDes = new AOrderedListSerializerDeserializer(
            new AOrderedListType(BuiltinType.AUUID, null));

    private IPointable inputArg0 = new VoidPointable();
    private IPointable inputArg1 = new VoidPointable();
    private IPointable inputArg2 = new VoidPointable();
    private IScalarEvaluator eval0;
    private IScalarEvaluator eval1;
    private IScalarEvaluator eval2;
    private final ActiveManager activeManager;
    private final EntityId entityId;

    public NotifyBrokerRuntime(IHyracksTaskContext ctx, IScalarEvaluatorFactory brokerEvalFactory,
            IScalarEvaluatorFactory subEvalFactory, IScalarEvaluatorFactory channelExecutionEvalFactory,
            EntityId activeJobId) throws HyracksDataException {
        this.tRef = new FrameTupleReference();
        eval0 = brokerEvalFactory.createScalarEvaluator(ctx);
        eval1 = subEvalFactory.createScalarEvaluator(ctx);
        eval2 = channelExecutionEvalFactory.createScalarEvaluator(ctx);
        this.activeManager = (ActiveManager) ((IAppRuntimeContext) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject()).getActiveManager();
        this.entityId = activeJobId;
    }

    @Override
    public void open() throws HyracksDataException {
        return;
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

            int serBrokerOffset = inputArg0.getStartOffset();
            bbis.setByteBuffer(tRef.getFrameTupleAccessor().getBuffer(), serBrokerOffset + 1);
            AString endpoint = AStringSerializerDeserializer.INSTANCE.deserialize(di);

            int serSubOffset = inputArg1.getStartOffset();
            bbis.setByteBuffer(tRef.getFrameTupleAccessor().getBuffer(), serSubOffset + 1);
            AOrderedList subs = subSerDes.deserialize(di);

            int resultSetOffset = inputArg2.getStartOffset();
            bbis.setByteBuffer(tRef.getFrameTupleAccessor().getBuffer(), resultSetOffset + 1);
            ADateTime executionTime = ADateTimeSerializerDeserializer.INSTANCE.deserialize(di);
            String executionTimeString;
            try {
                executionTimeString = executionTime.toSimpleString();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }

            ChannelJobService.sendBrokerNotificationsForChannel(entityId, endpoint.getStringValue(), subs,
                    executionTimeString);

        }

    }

    @Override
    public void close() throws HyracksDataException {
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
