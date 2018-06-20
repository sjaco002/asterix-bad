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

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.algebra.extension.ExtensionStatement;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.lang.BADParserFactory;
import org.apache.asterix.bad.metadata.Broker;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.UpsertStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ChannelSubscribeStatement extends ExtensionStatement {

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
    public byte getCategory() {
        return Category.QUERY;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId)
            throws HyracksDataException, AlgebricksException {
        String dataverse = ((QueryTranslator) statementExecutor).getActiveDataverse(dataverseName);
        String brokerDataverse = ((QueryTranslator) statementExecutor).getActiveDataverse(brokerDataverseName);

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

            String channelSubscriptionsDataset =
                    channel.getChannelId().getDataverse() + "." + channel.getChannelSubscriptionsDataset();

            String brokerSubscriptionsDataset =
                    channel.getChannelId().getDataverse() + "." + channel.getBrokerSubscriptionsDataset();

            if (argList.size() != channel.getFunction().getArity()) {
                throw new AsterixException("Channel expected " + channel.getFunction().getArity()
                        + " parameters but got " + argList.size());
            }

            final ResultDelivery resultDelivery = requestParameters.getResultProperties().getDelivery();
            final IHyracksDataset hdc = requestParameters.getHyracksDataset();
            final Stats stats = requestParameters.getStats();
            if (subscriptionId == null) {
                //Create a new subscription
                createChannelSubscription(statementExecutor, metadataProvider, hcc, hdc, stats,
                        channelSubscriptionsDataset, resultDelivery);

                metadataProvider.getLocks().reset();

                metadataProvider.setResultSetId(new ResultSetId(resultSetId));
                boolean resultsAsync =
                        resultDelivery == ResultDelivery.ASYNC || resultDelivery == ResultDelivery.DEFERRED;
                metadataProvider.setResultAsyncMode(resultsAsync);
                metadataProvider.setMaxResultReads(requestParameters.getResultProperties().getMaxReads());

                createBrokerSubscription(statementExecutor, metadataProvider, hcc, hdc, stats,
                        channelSubscriptionsDataset, brokerSubscriptionsDataset, broker, resultDelivery);

            } else {

                Query subscriptionTuple = new Query(false);

                List<FieldBinding> fb = new ArrayList<>();
                LiteralExpr leftExpr = new LiteralExpr(new StringLiteral(BADConstants.DataverseName));
                Expression rightExpr = new LiteralExpr(new StringLiteral(brokerDataverse));
                fb.add(new FieldBinding(leftExpr, rightExpr));

                leftExpr = new LiteralExpr(new StringLiteral(BADConstants.BrokerName));
                rightExpr = new LiteralExpr(new StringLiteral(broker.getBrokerName()));
                fb.add(new FieldBinding(leftExpr, rightExpr));

                leftExpr = new LiteralExpr(new StringLiteral(BADConstants.ChannelSubscriptionId));

                List<Expression> UUIDList = new ArrayList<>();
                UUIDList.add(new LiteralExpr(new StringLiteral(subscriptionId)));
                FunctionIdentifier function = BuiltinFunctions.UUID_CONSTRUCTOR;
                FunctionSignature UUIDfunc =
                        new FunctionSignature(function.getNamespace(), function.getName(), function.getArity());
                CallExpr UUIDCall = new CallExpr(UUIDfunc, UUIDList);

                rightExpr = UUIDCall;
                fb.add(new FieldBinding(leftExpr, rightExpr));

                for (int i = 0; i < argList.size(); i++) {
                    leftExpr = new LiteralExpr(new StringLiteral("param" + i));
                    rightExpr = argList.get(i);
                    fb.add(new FieldBinding(leftExpr, rightExpr));
                }
                RecordConstructor recordCon = new RecordConstructor(fb);
                subscriptionTuple.setBody(recordCon);
                subscriptionTuple.setVarCounter(varCounter);
                MetadataProvider tempMdProvider = new MetadataProvider(metadataProvider.getApplicationContext(),
                        metadataProvider.getDefaultDataverse());
                tempMdProvider.getConfig().putAll(metadataProvider.getConfig());

                //To update an existing subscription
                UpsertStatement upsert = new UpsertStatement(new Identifier(dataverse),
                        new Identifier(channel.getBrokerSubscriptionsDataset()), subscriptionTuple, varCounter, null,
                        null);
                ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(tempMdProvider, upsert, hcc, hdc,
                        resultDelivery, null, stats, false, null, null, null);
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            QueryTranslator.abort(e, e, mdTxnCtx);
            throw HyracksDataException.create(e);
        } finally {
            metadataProvider.getLocks().unlock();
        }

    }

    private void createChannelSubscription(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IHyracksDataset hdc, Stats stats, String channelSubscriptionsDataset,
            ResultDelivery resultDelivery) throws Exception {
        //TODO: Might be better to create the entire expression manually rather than parsing a string
        StringBuilder builder = new StringBuilder();
        builder.append("upsert into " + channelSubscriptionsDataset + "(\n");
        builder.append("(let v = (select value s from " + channelSubscriptionsDataset + " s where ");
        for (int i = 0; i < argList.size(); i++) {
            String quote = ((LiteralExpr) argList.get(i)).getValue() instanceof StringLiteral ? "\"" : "";
            builder.append("param" + i + " =  " + quote + ((LiteralExpr) argList.get(i)).getValue() + quote);
            if (i < argList.size() - 1) {
                builder.append(" and ");
            }
        }
        builder.append(")\n");
        builder.append("select value (CASE (array_count(v) > 0)\n");
        builder.append("WHEN true THEN {\"" + BADConstants.ChannelSubscriptionId + "\":v[0]."
                + BADConstants.ChannelSubscriptionId + ", ");
        for (int i = 0; i < argList.size(); i++) {
            String quote = ((LiteralExpr) argList.get(i)).getValue() instanceof StringLiteral ? "\"" : "";
            builder.append("\"param" + i + "\": " + quote + ((LiteralExpr) argList.get(i)).getValue() + quote);
            if (i < argList.size() - 1) {
                builder.append(", ");
            }
        }
        builder.append("}\n");
        builder.append("ELSE {\"" + BADConstants.ChannelSubscriptionId + "\":create_uuid(), ");
        for (int i = 0; i < argList.size(); i++) {
            String quote = ((LiteralExpr) argList.get(i)).getValue() instanceof StringLiteral ? "\"" : "";
            builder.append("\"param" + i + "\": " + quote + ((LiteralExpr) argList.get(i)).getValue() + quote);
            if (i < argList.size() - 1) {
                builder.append(", ");
            }
        }
        builder.append("}\n");
        builder.append("END))\n");
        builder.append(");");
        BADParserFactory factory = new BADParserFactory();
        List<Statement> fStatements = factory.createParser(new StringReader(builder.toString())).parse();
        ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider, fStatements.get(0), hcc,
                hdc, resultDelivery, null, stats, false, null, null, null);
    }

    private void createBrokerSubscription(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IHyracksDataset hdc, Stats stats, String channelSubscriptionsDataset,
            String brokerSubscriptionDataset, Broker broker, ResultDelivery resultDelivery) throws Exception {
        //TODO: Might be better to create the entire expression manually rather than parsing a string
        StringBuilder builder = new StringBuilder();
        builder.append("insert into " + brokerSubscriptionDataset + " as r (\n");
        builder.append("(select value {\"" + BADConstants.ChannelSubscriptionId + "\":s."
                + BADConstants.ChannelSubscriptionId + ",\"" + BADConstants.BrokerSubscriptionId
                + "\":create_uuid(), \"" + BADConstants.DataverseName + "\":\"" + broker.getDataverseName() + "\", \""
                + BADConstants.BrokerName + "\":\"" + broker.getBrokerName() + "\"}\n");
        builder.append("from " + channelSubscriptionsDataset + " s where ");
        for (int i = 0; i < argList.size(); i++) {
            String quote = ((LiteralExpr) argList.get(i)).getValue() instanceof StringLiteral ? "\"" : "";
            builder.append("param" + i + " =  " + quote + ((LiteralExpr) argList.get(i)).getValue() + quote);
            if (i < argList.size() - 1) {
                builder.append(" and ");
            }
        }
        builder.append(")\n");
        builder.append(") returning r." + BADConstants.BrokerSubscriptionId + ";");
        BADParserFactory factory = new BADParserFactory();
        List<Statement> fStatements = factory.createParser(new StringReader(builder.toString())).parse();
        ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider, fStatements.get(0), hcc,
                hdc, resultDelivery, null, stats, false, null, null, null);
    }

}