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

import org.apache.asterix.active.EntityId;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class NotifyBrokerRuntimeFactory implements IPushRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory brokerEvalFactory;
    private final IScalarEvaluatorFactory subEvalFactory;
    private final IScalarEvaluatorFactory channelExecutionEvalFactory;
    private final EntityId entityId;

    public NotifyBrokerRuntimeFactory(IScalarEvaluatorFactory brokerEvalFactory, IScalarEvaluatorFactory subEvalFactory,
            IScalarEvaluatorFactory channelExecutionEvalFactory, EntityId entityId) {
        this.brokerEvalFactory = brokerEvalFactory;
        this.subEvalFactory = subEvalFactory;
        this.channelExecutionEvalFactory = channelExecutionEvalFactory;
        this.entityId = entityId;
    }

    @Override
    public String toString() {
        return "notify-broker";
    }

    @Override
    public IPushRuntime createPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
        return new NotifyBrokerRuntime(ctx, brokerEvalFactory, subEvalFactory, channelExecutionEvalFactory, entityId);
    }
}
