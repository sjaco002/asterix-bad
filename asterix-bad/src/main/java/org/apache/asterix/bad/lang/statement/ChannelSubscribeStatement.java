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
package org.apache.asterix.bad.lang.statement;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.algebra.extension.IExtensionStatement;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.metadata.Broker;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.UpsertStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ChannelSubscribeStatement implements IExtensionStatement {

    private final Identifier dataverseName;
    private final Identifier channelName;
    private final Identifier brokerDataverseName;
    private final Identifier brokerName;
    private final List<Expression> argList;
    private final String subscriptionId;
    private final int varCounter;

    public ChannelSubscribeStatement(Identifier dataverseName, Identifier channelName, List<Expression> argList,
            int varCounter, Identifier brokerDataverseName, Identifier brokerName, String subscriptionId) {
        this.channelName = channelName;
        this.dataverseName = dataverseName;
        this.brokerDataverseName = brokerDataverseName;
        this.brokerName = brokerName;
        this.argList = argList;
        this.subscriptionId = subscriptionId;
        this.varCounter = varCounter;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getBrokerDataverseName() {
        return brokerDataverseName;
    }

    public Identifier getChannelName() {
        return channelName;
    }

    public Identifier getBrokerName() {
        return brokerName;
    }

    public List<Expression> getArgList() {
        return argList;
    }

    public int getVarCounter() {
        return varCounter;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    @Override
    public byte getKind() {
        return Kind.EXTENSION;
    }

    @Override
    public byte getCategory() {
        return Category.QUERY;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return null;
    }

    @Override
    public void handle(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery, Stats stats,
            int resultSetIdCounter) throws HyracksDataException, AlgebricksException {

        String dataverse = ((QueryTranslator) statementExecutor).getActiveDataverse(dataverseName);
        String brokerDataverse = ((QueryTranslator) statementExecutor)
.getActiveDataverse(brokerDataverseName);

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();

            Channel channel = BADLangExtension.getChannel(mdTxnCtx, dataverse, channelName.getValue());
            if (channel == null) {
                throw new AsterixException("There is no channel with this name " + channelName + ".");
            }
            Broker broker = BADLangExtension.getBroker(mdTxnCtx, brokerDataverse, brokerName.getValue());
            if (broker == null) {
                throw new AsterixException("There is no broker with this name " + brokerName + ".");
            }

            String subscriptionsDatasetName = channel.getSubscriptionsDataset();

            if (argList.size() != channel.getFunction().getArity()) {
                throw new AsterixException("Channel expected " + channel.getFunction().getArity()
                        + " parameters but got " + argList.size());
            }

            Query subscriptionTuple = new Query(false);

            List<FieldBinding> fb = new ArrayList<FieldBinding>();
            LiteralExpr leftExpr = new LiteralExpr(new StringLiteral(BADConstants.DataverseName));
            Expression rightExpr = new LiteralExpr(new StringLiteral(brokerDataverse));
            fb.add(new FieldBinding(leftExpr, rightExpr));

            leftExpr = new LiteralExpr(new StringLiteral(BADConstants.BrokerName));
            rightExpr = new LiteralExpr(new StringLiteral(broker.getBrokerName()));
            fb.add(new FieldBinding(leftExpr, rightExpr));

            if (subscriptionId != null) {
                leftExpr = new LiteralExpr(new StringLiteral(BADConstants.SubscriptionId));

                List<Expression> UUIDList = new ArrayList<Expression>();
                UUIDList.add(new LiteralExpr(new StringLiteral(subscriptionId)));
                FunctionIdentifier function = BuiltinFunctions.UUID_CONSTRUCTOR;
                FunctionSignature UUIDfunc = new FunctionSignature(function.getNamespace(), function.getName(),
                        function.getArity());
                CallExpr UUIDCall = new CallExpr(UUIDfunc, UUIDList);

                rightExpr = UUIDCall;
                fb.add(new FieldBinding(leftExpr, rightExpr));
            }

            for (int i = 0; i < argList.size(); i++) {
                leftExpr = new LiteralExpr(new StringLiteral("param" + i));
                rightExpr = argList.get(i);
                fb.add(new FieldBinding(leftExpr, rightExpr));
            }
            RecordConstructor recordCon = new RecordConstructor(fb);
            subscriptionTuple.setBody(recordCon);

            subscriptionTuple.setVarCounter(varCounter);

            if (subscriptionId == null) {

                VariableExpr subscriptionVar = new VariableExpr(new VarIdentifier("$sub", 1));
                VariableExpr useSubscriptionVar = new VariableExpr(new VarIdentifier("$sub", 1));
                VariableExpr resultVar = new VariableExpr(new VarIdentifier("$result", 0));
                VariableExpr useResultVar = new VariableExpr(new VarIdentifier("$result", 0));
                useResultVar.setIsNewVar(false);
                useSubscriptionVar.setIsNewVar(false);
                List<Clause> clauseList = new ArrayList<>();
                LetClause let = new LetClause(subscriptionVar,
                        new FieldAccessor(useResultVar, new Identifier(BADConstants.SubscriptionId)));
                clauseList.add(let);
                FLWOGRExpression body = new FLWOGRExpression(clauseList, useSubscriptionVar);

                metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter++));
                metadataProvider.setResultAsyncMode(
                        resultDelivery == ResultDelivery.ASYNC || resultDelivery == ResultDelivery.DEFERRED);
                InsertStatement insert = new InsertStatement(new Identifier(dataverse),
                        new Identifier(subscriptionsDatasetName), subscriptionTuple, varCounter, resultVar,
 body);
                ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider, insert, hcc, hdc,
                        resultDelivery, stats, false);
            } else {
                UpsertStatement upsert = new UpsertStatement(new Identifier(dataverse),
                        new Identifier(subscriptionsDatasetName), subscriptionTuple, varCounter, null, null);
                ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider, upsert, hcc, hdc,
                        resultDelivery, stats, false);
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            QueryTranslator.abort(e, e, mdTxnCtx);
            throw new HyracksDataException(e);
        }

    }

}