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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.algebra.extension.ExtensionStatement;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.BADJobService;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.lang.BADParserFactory;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener.PrecompiledType;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.transactions.ITxnIdFactory;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.IndexedTypeExpression;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.CreateIndexStatement;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.IDatasetDetailsDecl;
import org.apache.asterix.lang.common.statement.InternalDetailsDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.SetStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.base.temporal.ADurationParserFactory;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;

public class CreateChannelStatement extends ExtensionStatement {

    private static final Logger LOGGER = Logger.getLogger(CreateChannelStatement.class.getName());
    private final Identifier channelName;
    private final FunctionSignature function;
    private final CallExpr period;
    private Identifier dataverseName;
    private String duration;
    private String body;
    private String subscriptionsTableName;
    private String resultsTableName;
    private String dataverse;
    private final boolean push;

    public CreateChannelStatement(Identifier dataverseName, Identifier channelName, FunctionSignature function,
            Expression period, boolean push) {
        this.channelName = channelName;
        this.dataverseName = dataverseName;
        this.function = function;
        this.period = (CallExpr) period;
        this.duration = "";
        this.push = push;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getChannelName() {
        return channelName;
    }

    public String getResultsName() {
        return resultsTableName;
    }

    public String getSubscriptionsName() {
        return subscriptionsTableName;
    }

    public String getDuration() {
        return duration;
    }

    public FunctionSignature getFunction() {
        return function;
    }

    public Expression getPeriod() {
        return period;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

    public void initialize(MetadataTransactionContext mdTxnCtx)
            throws AlgebricksException, HyracksDataException {
        Function lookup = MetadataManager.INSTANCE.getFunction(mdTxnCtx, function);
        if (lookup == null) {
            throw new MetadataException(" Unknown function " + function.getName());
        }

        if (!period.getFunctionSignature().getName().equals("duration")) {
            throw new MetadataException(
                    "Expected argument period as a duration, but got " + period.getFunctionSignature().getName() + ".");
        }
        duration = ((StringLiteral) ((LiteralExpr) period.getExprList().get(0)).getValue()).getValue();
        IValueParser durationParser = ADurationParserFactory.INSTANCE.createValueParser();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(bos);
        durationParser.parse(duration.toCharArray(), 0, duration.toCharArray().length, outputStream);
    }

    private void createDatasets(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc) throws AsterixException, Exception {

        Identifier subscriptionsTypeName = new Identifier(BADConstants.ChannelSubscriptionsType);
        Identifier resultsTypeName = new Identifier(BADConstants.ChannelResultsType);
        //Setup the subscriptions dataset
        List<List<String>> partitionFields = new ArrayList<>();
        List<Integer> keyIndicators = new ArrayList<>();
        keyIndicators.add(0);
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add(BADConstants.SubscriptionId);
        partitionFields.add(fieldNames);
        IDatasetDetailsDecl idd = new InternalDetailsDecl(partitionFields, keyIndicators, true, null);
        DatasetDecl createSubscriptionsDataset = new DatasetDecl(dataverseName, new Identifier(subscriptionsTableName),
                new Identifier(BADConstants.BAD_DATAVERSE_NAME), subscriptionsTypeName, null, null, null,
                new HashMap<String, String>(), DatasetType.INTERNAL, idd, null, true);

        //Setup the results dataset
        partitionFields = new ArrayList<>();
        fieldNames = new ArrayList<>();
        fieldNames.add(BADConstants.ResultId);
        partitionFields.add(fieldNames);
        idd = new InternalDetailsDecl(partitionFields, keyIndicators, true, null);
        DatasetDecl createResultsDataset = new DatasetDecl(dataverseName, new Identifier(resultsTableName),
                new Identifier(BADConstants.BAD_DATAVERSE_NAME), resultsTypeName, null, null, null,
                new HashMap<String, String>(), DatasetType.INTERNAL, idd, null, true);

        //Create an index on timestamp for results
        CreateIndexStatement createTimeIndex = new CreateIndexStatement();
        createTimeIndex.setDatasetName(new Identifier(resultsTableName));
        createTimeIndex.setDataverseName(dataverseName);
        createTimeIndex.setIndexName(new Identifier(resultsTableName + "TimeIndex"));
        createTimeIndex.setIfNotExists(false);
        createTimeIndex.setIndexType(IndexType.BTREE);
        createTimeIndex.setEnforced(false);
        createTimeIndex.setGramLength(0);
        List<String> fNames = new ArrayList<>();
        fNames.add(BADConstants.ChannelExecutionTime);
        Pair<List<String>, IndexedTypeExpression> fields = new Pair<>(fNames, null);
        createTimeIndex.addFieldExprPair(fields);
        createTimeIndex.addFieldIndexIndicator(0);


        //Run both statements to create datasets
        ((QueryTranslator) statementExecutor).handleCreateDatasetStatement(metadataProvider, createSubscriptionsDataset,
                hcc, null);
        metadataProvider.getLocks().reset();
        ((QueryTranslator) statementExecutor).handleCreateDatasetStatement(metadataProvider, createResultsDataset, hcc,
                null);
        metadataProvider.getLocks().reset();

        //Create a time index for the results
        ((QueryTranslator) statementExecutor).handleCreateIndexStatement(metadataProvider, createTimeIndex, hcc, null);

    }

    private JobSpecification createChannelJob(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IHyracksDataset hdc, Stats stats) throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("SET inline_with \"false\";\n");
        if (!push) {
            builder.append("insert into " + dataverse + "." + resultsTableName);
            builder.append(" as a (\n");
        }
        builder.append("with " + BADConstants.ChannelExecutionTime + " as current_datetime() \n");
        builder.append("select result, ");
        builder.append(BADConstants.ChannelExecutionTime + ", ");
        builder.append("sub." + BADConstants.SubscriptionId + " as " + BADConstants.SubscriptionId + ",");
        builder.append("current_datetime() as " + BADConstants.DeliveryTime + "\n");
        builder.append("from " + dataverse + "." + subscriptionsTableName + " sub,\n");
        builder.append(BADConstants.BAD_DATAVERSE_NAME + "." + BADConstants.BROKER_KEYWORD + " b, \n");
        builder.append(function.getNamespace() + "." + function.getName() + "(");
        int i = 0;
        for (; i < function.getArity() - 1; i++) {
            builder.append("sub.param" + i + ",");
        }
        builder.append("sub.param" + i + ") result \n");
        builder.append("where b." + BADConstants.BrokerName + " = sub." + BADConstants.BrokerName + "\n");
        builder.append("and b." + BADConstants.DataverseName + " = sub." + BADConstants.DataverseName + "\n");
        if (!push) {
            builder.append(")");
            builder.append(" returning a");
        }
        builder.append(";");
        body = builder.toString();
        BADParserFactory factory = new BADParserFactory();
        List<Statement> fStatements = factory.createParser(new StringReader(builder.toString())).parse();

        SetStatement ss = (SetStatement) fStatements.get(0);
        metadataProvider.getConfig().put(ss.getPropName(), ss.getPropValue());
        if (push) {
            return BADJobService.compilePushChannel(statementExecutor, metadataProvider, hcc,
                    (Query) fStatements.get(1));
        }
        return ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider, fStatements.get(1),
                hcc, hdc, ResultDelivery.ASYNC, null, stats, true, null);
    }

    private void setupExecutorJob(EntityId entityId, JobSpecification channeljobSpec, IHyracksClientConnection hcc,
            DeployedJobSpecEventListener listener, ITxnIdFactory txnIdFactory) throws Exception {
        if (channeljobSpec != null) {
            DeployedJobSpecId destributedId = hcc.deployJobSpec(channeljobSpec);
            ScheduledExecutorService ses = BADJobService.startRepetitiveDeployedJobSpec(destributedId, hcc,
                    BADJobService.findPeriod(duration), new HashMap<>(), entityId, txnIdFactory, listener);
            listener.storeDistributedInfo(destributedId, ses, null, null);
        }

    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestContext, MetadataProvider metadataProvider, int resultSetId)
            throws HyracksDataException, AlgebricksException {
        //This function performs three tasks:
        //1. Create datasets for the Channel
        //2. Create and run the Channel Job
        //3. Create the metadata entry for the channel

        //TODO: Figure out how to handle when a subset of the 3 tasks fails

        dataverseName = new Identifier(((QueryTranslator) statementExecutor).getActiveDataverse(dataverseName));
        dataverse = dataverseName.getValue();
        subscriptionsTableName = channelName + BADConstants.subscriptionEnding;
        resultsTableName = channelName + BADConstants.resultsEnding;

        EntityId entityId = new EntityId(BADConstants.CHANNEL_EXTENSION_NAME, dataverse, channelName.getValue());
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        DeployedJobSpecEventListener listener = (DeployedJobSpecEventListener) activeEventHandler.getListener(entityId);
        boolean alreadyActive = false;
        Channel channel = null;

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            channel = BADLangExtension.getChannel(mdTxnCtx, dataverse, channelName.getValue());
            if (channel != null) {
                throw new AlgebricksException("A channel with this name " + channelName + " already exists.");
            }
            if (listener != null) {
                alreadyActive = listener.isActive();
            }
            if (alreadyActive) {
                throw new AsterixException("Channel " + channelName + " is already running");
            }
            initialize(mdTxnCtx);

            //check if names are available before creating anything
            if (MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverse, subscriptionsTableName) != null) {
                throw new AsterixException("The channel name:" + channelName + " is not available.");
            }
            if (MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverse, resultsTableName) != null) {
                throw new AsterixException("The channel name:" + channelName + " is not available.");
            }
            MetadataProvider tempMdProvider = new MetadataProvider(metadataProvider.getApplicationContext(),
                    metadataProvider.getDefaultDataverse());
            tempMdProvider.getConfig().putAll(metadataProvider.getConfig());
            final IHyracksDataset hdc = requestContext.getHyracksDataset();
            final Stats stats = requestContext.getStats();
            //Create Channel Datasets
            createDatasets(statementExecutor, tempMdProvider, hcc);
            tempMdProvider.getLocks().reset();
            //Create Channel Internal Job
            JobSpecification channeljobSpec = createChannelJob(statementExecutor, tempMdProvider, hcc, hdc, stats);

            // Now we subscribe
            if (listener == null) {
                listener = new DeployedJobSpecEventListener(appCtx, entityId,
                        push ? PrecompiledType.PUSH_CHANNEL : PrecompiledType.CHANNEL, null,
                        "BadListener");
                activeEventHandler.registerListener(listener);
            }

            setupExecutorJob(entityId, channeljobSpec, hcc, listener, metadataProvider.getTxnIdFactory());
            channel = new Channel(dataverse, channelName.getValue(), subscriptionsTableName, resultsTableName, function,
                    duration, null, body);

            MetadataManager.INSTANCE.addEntity(mdTxnCtx, channel);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (mdTxnCtx != null) {
                QueryTranslator.abort(e, e, mdTxnCtx);
            }
            LOGGER.log(Level.WARNING, "Failed creating a channel", e);
            throw HyracksDataException.create(e);
        } finally {
            metadataProvider.getLocks().unlock();
        }

    }

}