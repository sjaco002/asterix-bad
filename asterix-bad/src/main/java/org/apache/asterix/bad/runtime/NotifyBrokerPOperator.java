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
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class NotifyBrokerPOperator extends AbstractPhysicalOperator {

    private final EntityId entityId;

    public NotifyBrokerPOperator(EntityId entityId) {
        this.entityId = entityId;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.DELEGATE_OPERATOR;
    }

    @Override
    public String toString() {
        return "NOTIFY_BROKERS";
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        return emptyUnaryRequirements();
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
                    throws AlgebricksException {
        DelegateOperator notify = (DelegateOperator) op;
        LogicalVariable subVar = ((NotifyBrokerOperator) notify.getDelegate()).getSubscriptionVariable();
        LogicalVariable brokerVar = ((NotifyBrokerOperator) notify.getDelegate()).getBrokerEndpointVariable();
        LogicalVariable executionVar = ((NotifyBrokerOperator) notify.getDelegate()).getChannelExecutionVariable();

        int brokerColumn = inputSchemas[0].findVariable(brokerVar);
        int subColumn = inputSchemas[0].findVariable(subVar);
        int executionColumn = inputSchemas[0].findVariable(executionVar);

        IScalarEvaluatorFactory brokerEvalFactory = new ColumnAccessEvalFactory(brokerColumn);
        IScalarEvaluatorFactory subEvalFactory = new ColumnAccessEvalFactory(subColumn);
        IScalarEvaluatorFactory channelExecutionEvalFactory = new ColumnAccessEvalFactory(executionColumn);

        NotifyBrokerRuntimeFactory runtime = new NotifyBrokerRuntimeFactory(brokerEvalFactory, subEvalFactory,
                channelExecutionEvalFactory, entityId);

        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema,
                context);

        builder.contributeMicroOperator(op, runtime, recDesc);

        // and contribute one edge from its child
        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, notify, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }
}
