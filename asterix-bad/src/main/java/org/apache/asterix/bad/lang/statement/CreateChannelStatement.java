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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveJobNotificationHandler;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.algebra.extension.IExtensionStatement;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.ChannelJobInfo;
import org.apache.asterix.bad.ChannelJobService;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.bad.metadata.ChannelEventsListener;
import org.apache.asterix.bad.runtime.RepetitiveChannelOperatorDescriptor;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.external.feed.api.IActiveLifecycleEventSubscriber;
import org.apache.asterix.external.feed.api.IActiveLifecycleEventSubscriber.ActiveLifecycleEvent;
import org.apache.asterix.external.feed.management.ActiveLifecycleEventSubscriber;
import org.apache.asterix.file.JobSpecificationUtils;
import org.apache.asterix.lang.aql.parser.AQLParserFactory;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.IDatasetDetailsDecl;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.InternalDetailsDecl;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.base.temporal.ADurationParserFactory;
import org.apache.asterix.runtime.util.AppContextInfo;
import org.apache.asterix.runtime.util.ClusterStateManager;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.util.JobUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.ICCApplicationContext;
import org.apache.hyracks.api.client.ClusterControllerInfo;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;

public class CreateChannelStatement implements IExtensionStatement {

    private static final Logger LOGGER = Logger.getLogger(CreateChannelStatement.class.getName());

    private final Identifier dataverseName;
    private final Identifier channelName;
    private final FunctionSignature function;
    private final CallExpr period;
    private String duration;
    private InsertStatement channelResultsInsertQuery;
    private String subscriptionsTableName;
    private String resultsTableName;
    private boolean distributed;

    public CreateChannelStatement(Identifier dataverseName, Identifier channelName, FunctionSignature function,
            Expression period, boolean distributed) {
        this.channelName = channelName;
        this.dataverseName = dataverseName;
        this.function = function;
        this.period = (CallExpr) period;
        this.duration = "";
        this.distributed = distributed;
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

    public InsertStatement getChannelResultsInsertQuery() {
        return channelResultsInsertQuery;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return null;
    }

    public void initialize(MetadataTransactionContext mdTxnCtx, String subscriptionsTableName, String resultsTableName)
            throws MetadataException, HyracksDataException {
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
        this.resultsTableName = resultsTableName;
        this.subscriptionsTableName = subscriptionsTableName;

    }

    @Override
    public byte getKind() {
        return Kind.EXTENSION;
    }

    public Pair<JobSpecification, AlgebricksAbsolutePartitionConstraint> buildChannelJobSpec(String dataverse,
            String channelName, String duration, MetadataProvider metadataProvider, JobSpecification channeljobSpec,
            String strIP, int port) throws Exception {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IOperatorDescriptor channelQueryExecuter;
        AlgebricksPartitionConstraint executerPc;

        Pair<IOperatorDescriptor, AlgebricksAbsolutePartitionConstraint> p = buildChannelRuntime(spec, dataverse,
                channelName, duration, channeljobSpec, strIP, port);
        channelQueryExecuter = p.first;
        executerPc = p.second;
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, channelQueryExecuter, executerPc);
        spec.addRoot(channelQueryExecuter);
        return new Pair<>(spec, p.second);

    }

    public Pair<IOperatorDescriptor, AlgebricksAbsolutePartitionConstraint> buildChannelRuntime(
            JobSpecification jobSpec, String dataverse, String channelName, String duration,
            JobSpecification channeljobSpec, String strIP, int port) throws Exception {
        RepetitiveChannelOperatorDescriptor channelOp = new RepetitiveChannelOperatorDescriptor(jobSpec, dataverse,
                channelName, duration, channeljobSpec, strIP, port);

        String partition = ClusterStateManager.INSTANCE.getClusterLocations().getLocations()[0];
        Set<String> ncs = new HashSet<>(Arrays.asList(partition));
        AlgebricksAbsolutePartitionConstraint partitionConstraint = new AlgebricksAbsolutePartitionConstraint(
                ncs.toArray(new String[ncs.size()]));
        return new Pair<IOperatorDescriptor, AlgebricksAbsolutePartitionConstraint>(channelOp, partitionConstraint);
    }

    private void createDatasets(IStatementExecutor statementExecutor, Identifier subscriptionsName,
            Identifier resultsName, MetadataProvider metadataProvider, IHyracksClientConnection hcc,
            IHyracksDataset hdc, Stats stats, String dataverse) throws AsterixException, Exception {

        Identifier subscriptionsTypeName = new Identifier(BADConstants.ChannelSubscriptionsType);
        Identifier resultsTypeName = new Identifier(BADConstants.ChannelResultsType);
        //Setup the subscriptions dataset
        List<List<String>> partitionFields = new ArrayList<List<String>>();
        List<Integer> keyIndicators = new ArrayList<Integer>();
        keyIndicators.add(0);
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add(BADConstants.SubscriptionId);
        partitionFields.add(fieldNames);
        IDatasetDetailsDecl idd = new InternalDetailsDecl(partitionFields, keyIndicators, true, null, false);
        DatasetDecl createSubscriptionsDataset = new DatasetDecl(new Identifier(dataverse), subscriptionsName,
                new Identifier(BADConstants.BAD_DATAVERSE_NAME), subscriptionsTypeName, null, null, null, null,
                new HashMap<String, String>(), new HashMap<String, String>(), DatasetType.INTERNAL, idd, true);

        //Setup the results dataset
        partitionFields = new ArrayList<List<String>>();
        fieldNames = new ArrayList<String>();
        fieldNames.add(BADConstants.ResultId);
        partitionFields.add(fieldNames);
        idd = new InternalDetailsDecl(partitionFields, keyIndicators, true, null, false);
        DatasetDecl createResultsDataset = new DatasetDecl(new Identifier(dataverse), resultsName,
                new Identifier(BADConstants.BAD_DATAVERSE_NAME), resultsTypeName, null, null, null, null,
                new HashMap<String, String>(), new HashMap<String, String>(), DatasetType.INTERNAL, idd, true);

        //Run both statements to create datasets
        ((QueryTranslator) statementExecutor).handleCreateDatasetStatement(metadataProvider, createSubscriptionsDataset,
                hcc);
        ((QueryTranslator) statementExecutor).handleCreateDatasetStatement(metadataProvider, createResultsDataset, hcc);

    }

    private JobSpecification createChannelJob(IStatementExecutor statementExecutor, Identifier subscriptionsName,
            Identifier resultsName, MetadataProvider metadataProvider, IHyracksClientConnection hcc,
            IHyracksDataset hdc, Stats stats, String dataverse) throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("insert into dataset " + dataverse + "." + resultsName + " ");
        builder.append(" as $a (" + " let $" + BADConstants.ChannelExecutionTime + " := current-datetime() \n");

        builder.append("for $sub in dataset " + dataverse + "." + subscriptionsName + "\n");
        builder.append(
                "for $broker in dataset " + BADConstants.BAD_DATAVERSE_NAME + "." + BADConstants.BROKER_KEYWORD + "\n");
        builder.append("where $broker." + BADConstants.BrokerName + "= $sub." + BADConstants.BrokerName + "\n");
        builder.append("and $broker." + BADConstants.DataverseName + "= $sub." + BADConstants.DataverseName + "\n");
        builder.append(" for $result in " + function.getNamespace() + "." + function.getName() + "(");
        int i = 0;
        for (; i < function.getArity() - 1; i++) {
            builder.append("$sub.param" + i + ",");
        }
        builder.append("$sub.param" + i + ")\n");
        builder.append("return {\n");
        builder.append("\"" + BADConstants.ChannelExecutionTime + "\":$" + BADConstants.ChannelExecutionTime + ",");
        builder.append("\"" + BADConstants.SubscriptionId + "\":$sub." + BADConstants.SubscriptionId + ",");
        builder.append("\"" + BADConstants.DeliveryTime + "\":current-datetime(),");
        builder.append("\"result\":$result");
        builder.append("}");
        builder.append(")");
        builder.append(" returning $a");
        builder.append(";");
        AQLParserFactory aqlFact = new AQLParserFactory();
        List<Statement> fStatements = aqlFact.createParser(new StringReader(builder.toString())).parse();
        return ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider, fStatements.get(0),
                hcc, hdc, ResultDelivery.ASYNC, stats, true);
    }

    private void setupCompiledJob(MetadataProvider metadataProvider, String dataverse, EntityId entityId,
            JobSpecification channeljobSpec, IHyracksClientConnection hcc) throws Exception {
        ICCApplicationContext iCCApp = AppContextInfo.INSTANCE.getCCApplicationContext();
        ClusterControllerInfo ccInfo = iCCApp.getCCContext().getClusterControllerInfo();
        String strIP = ccInfo.getClientNetAddress();
        int port = ccInfo.getClientNetPort();
        //Create Channel Operator
        Pair<JobSpecification, AlgebricksAbsolutePartitionConstraint> alteredJobSpec = buildChannelJobSpec(dataverse,
                channelName.getValue(), duration, metadataProvider, channeljobSpec, strIP, port);

        ChannelJobInfo channelJobInfo = new ChannelJobInfo(entityId, null, ActivityState.ACTIVE, alteredJobSpec.first);
        alteredJobSpec.first.setProperty(ActiveJobNotificationHandler.ACTIVE_ENTITY_PROPERTY_NAME, channelJobInfo);
        JobUtils.runJob(hcc, alteredJobSpec.first, false);
    }

    private void setupDistributedJob(EntityId entityId, JobSpecification channeljobSpec, IHyracksClientConnection hcc)
            throws Exception {
        ChannelJobInfo channelJobInfo = new ChannelJobInfo(entityId, null, ActivityState.ACTIVE, channeljobSpec);
        channeljobSpec.setProperty(ActiveJobNotificationHandler.ACTIVE_ENTITY_PROPERTY_NAME, channelJobInfo);
        JobId jobId = hcc.startJob(channeljobSpec, EnumSet.of(JobFlag.STORE_JOB));
        ChannelJobService.startJob(channeljobSpec, EnumSet.of(JobFlag.STORE_JOB), jobId, hcc,
                ChannelJobService.findPeriod(duration));
    }

    @Override
    public void handle(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery, Stats stats,
            int resultSetIdCounter) throws HyracksDataException, AlgebricksException {

        //This function performs three tasks:
        //1. Create datasets for the Channel
        //2. Create and run the Channel Job
        //3. Create the metadata entry for the channel

        //TODO: Figure out how to handle when a subset of the 3 tasks fails
        //TODO: The compiled job will break if anything changes on the function or two datasets
        // Need to make sure we do proper checking when altering these things

        String dataverse = ((QueryTranslator) statementExecutor).getActiveDataverse(dataverseName);

        Identifier subscriptionsName = new Identifier(channelName + BADConstants.subscriptionEnding);
        Identifier resultsName = new Identifier(channelName + BADConstants.resultsEnding);
        EntityId entityId = new EntityId(BADConstants.CHANNEL_EXTENSION_NAME, dataverse, channelName.getValue());
        ChannelEventsListener listener = (ChannelEventsListener) ActiveJobNotificationHandler.INSTANCE
                .getActiveEntityListener(entityId);
        IActiveLifecycleEventSubscriber eventSubscriber = new ActiveLifecycleEventSubscriber();
        boolean subscriberRegistered = false;
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
                subscriberRegistered = listener.isChannelActive(entityId, eventSubscriber);
            }
            if (subscriberRegistered) {
                throw new AsterixException("Channel " + channelName + " is already running");
            }
            initialize(mdTxnCtx, subscriptionsName.getValue(), resultsName.getValue());
            channel = new Channel(dataverse, channelName.getValue(), subscriptionsTableName, resultsTableName, function,
                    duration);

            //check if names are available before creating anything
            if (MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverse, subscriptionsName.getValue()) != null) {
                throw new AsterixException("The channel name:" + channelName + " is not available.");
            }
            if (MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverse, resultsName.getValue()) != null) {
                throw new AsterixException("The channel name:" + channelName + " is not available.");
            }

            // Now we subscribe
            if (listener == null) {
                listener = new ChannelEventsListener(entityId);
                ActiveJobNotificationHandler.INSTANCE.registerListener(listener);
            }
            listener.registerEventSubscriber(eventSubscriber);
            subscriberRegistered = true;

            //Create Channel Datasets
            createDatasets(statementExecutor, subscriptionsName, resultsName, metadataProvider, hcc, hdc, stats,
                    dataverse);

            //Create Channel Internal Job
            JobSpecification channeljobSpec = createChannelJob(statementExecutor, subscriptionsName, resultsName,
                    metadataProvider, hcc, hdc, stats, dataverse);

            if (distributed) {
                setupDistributedJob(entityId, channeljobSpec, hcc);
            } else {
                setupCompiledJob(metadataProvider, dataverse, entityId, channeljobSpec, hcc);
            }
            eventSubscriber.assertEvent(ActiveLifecycleEvent.ACTIVE_JOB_STARTED);

            MetadataManager.INSTANCE.addEntity(mdTxnCtx, channel);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (mdTxnCtx != null) {
                QueryTranslator.abort(e, e, mdTxnCtx);
            }
            LOGGER.log(Level.WARNING, "Failed creating a channel", e);
            throw new HyracksDataException(e);
        }

    }

}