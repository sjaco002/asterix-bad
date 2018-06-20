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
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
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
    private final List<String> paramList;
    private final String subscriptionId;
    private final int varCounter;

    public ChannelSubscribeStatement(Identifier dataverseName, Identifier channelName, List<Expression> argList,
            int varCounter, Identifier brokerDataverseName, Identifier brokerName, String subscriptionId,
            List<String> paramList) {
        this.channelName = channelName;
        this.dataverseName = dataverseName;
        this.brokerDataverseName = brokerDataverseName;
        this.brokerName = brokerName;
        this.argList = argList;
        this.subscriptionId = subscriptionId;
        this.varCounter = varCounter;
        this.paramList = paramList;
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

            final ResultDelivery resultDelivery = requestParameters.getResultProperties().getDelivery();
            final IHyracksDataset hdc = requestParameters.getHyracksDataset();
            final Stats stats = requestParameters.getStats();
            if (subscriptionId == null) {
                //Create a new subscription
                if (argList.size() != channel.getFunction().getArity()) {
                    throw new AsterixException("Channel expected " + channel.getFunction().getArity()
                            + " parameters but got " + argList.size());
                }

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
                //move subscription
                moveBrokerSubscription(statementExecutor, metadataProvider, hcc, hdc, stats, brokerSubscriptionsDataset,
                        broker, resultDelivery, subscriptionId);
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
        for (int i = 0; i < paramList.size(); i++) {
            builder.append("param" + i + " =  " + paramList.get(i));
            if (i < paramList.size() - 1) {
                builder.append(" and ");
            }
        }
        builder.append(")\n");
        builder.append("select value (CASE (array_count(v) > 0)\n");
        builder.append("WHEN true THEN {\"" + BADConstants.ChannelSubscriptionId + "\":v[0]."
                + BADConstants.ChannelSubscriptionId + ", ");
        for (int i = 0; i < paramList.size(); i++) {
            builder.append("\"param" + i + "\": " + paramList.get(i));
            if (i < paramList.size() - 1) {
                builder.append(", ");
            }
        }
        builder.append("}\n");
        builder.append("ELSE {\"" + BADConstants.ChannelSubscriptionId + "\":create_uuid(), ");
        for (int i = 0; i < paramList.size(); i++) {
            builder.append("\"param" + i + "\": " + paramList.get(i));
            if (i < paramList.size() - 1) {
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
        for (int i = 0; i < paramList.size(); i++) {
            builder.append("param" + i + " =  " + paramList.get(i));
            if (i < paramList.size() - 1) {
                builder.append(" and ");
            }
        }
        builder.append(" limit 1)\n");
        builder.append(") returning r." + BADConstants.BrokerSubscriptionId + ";");
        BADParserFactory factory = new BADParserFactory();
        List<Statement> fStatements = factory.createParser(new StringReader(builder.toString())).parse();
        ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider, fStatements.get(0), hcc,
                hdc, resultDelivery, null, stats, false, null, null, null);
    }

    private void moveBrokerSubscription(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IHyracksDataset hdc, Stats stats, String brokerSubscriptionDataset,
            Broker broker, ResultDelivery resultDelivery, String subscriptionString) throws Exception {
        //TODO: Might be better to create the entire expression manually rather than parsing a string
        StringBuilder builder = new StringBuilder();
        builder.append("upsert into " + brokerSubscriptionDataset + "(\n");
        builder.append(
                "(select value {\"" + BADConstants.ChannelSubscriptionId + "\":s." + BADConstants.ChannelSubscriptionId
                        + ",\"" + BADConstants.BrokerSubscriptionId + "\":s." + BADConstants.BrokerSubscriptionId
                        + ",\"" + BADConstants.DataverseName + "\":\"" + broker.getDataverseName() + "\", \""
                        + BADConstants.BrokerName + "\":\"" + broker.getBrokerName() + "\"}\n");
        builder.append("from " + brokerSubscriptionDataset + " s where ");
        builder.append("s." + BADConstants.BrokerSubscriptionId + " = uuid(\"" + subscriptionString + "\"))\n");
        builder.append(");");
        BADParserFactory factory = new BADParserFactory();
        List<Statement> fStatements = factory.createParser(new StringReader(builder.toString())).parse();
        ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider, fStatements.get(0), hcc,
                hdc, resultDelivery, null, stats, false, null, null, null);
    }

}