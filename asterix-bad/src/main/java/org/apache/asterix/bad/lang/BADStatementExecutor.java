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
package org.apache.asterix.bad.lang;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.bad.lang.statement.BrokerDropStatement;
import org.apache.asterix.bad.lang.statement.ChannelDropStatement;
import org.apache.asterix.bad.lang.statement.ProcedureDropStatement;
import org.apache.asterix.bad.metadata.Broker;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.bad.metadata.Procedure;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.statement.FunctionDropStatement;
import org.apache.asterix.lang.common.statement.IndexDropStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;

public class BADStatementExecutor extends QueryTranslator {

    public BADStatementExecutor(ICcApplicationContext appCtx, List<Statement> statements, SessionOutput output,
            ILangCompilationProvider compliationProvider, ExecutorService executorService) {
        super(appCtx, statements, output, compliationProvider, executorService);
    }

    //TODO: Most of this file could go away if we had metadata dependencies

    private void checkIfDatasetIsInUse(MetadataTransactionContext mdTxnCtx, String dataverse, String dataset)
            throws CompilationException, AlgebricksException {
        List<Channel> channels = BADLangExtension.getAllChannels(mdTxnCtx);
        for (Channel channel : channels) {
            List<List<List<String>>> dependencies = channel.getDependencies();
            List<List<String>> datasetDependencies = dependencies.get(0);
            for (List<String> dependency : datasetDependencies) {
                if (dependency.get(0).equals(dataverse) && dependency.get(1).equals(dataset)) {
                    throw new CompilationException("Cannot alter dataset " + dataverse + "." + dataset + ". "
                            + channel.getChannelId() + " depends on it!");
                }
            }

        }
        List<Procedure> procedures = BADLangExtension.getAllProcedures(mdTxnCtx);
        for (Procedure procedure : procedures) {
            List<List<List<String>>> dependencies = procedure.getDependencies();
            List<List<String>> datasetDependencies = dependencies.get(0);
            for (List<String> dependency : datasetDependencies) {
                if (dependency.get(0).equals(dataverse) && dependency.get(1).equals(dataset)) {
                    throw new CompilationException("Cannot alter dataset " + dataverse + "." + dataset + ". "
                            + procedure.getEntityId() + " depends on it!");
                }
            }

        }
    }

    @Override
    public void handleDatasetDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        String dvId = getActiveDataverse(((DropDatasetStatement) stmt).getDataverseName());
        Identifier dsId = ((DropDatasetStatement) stmt).getDatasetName();

        checkIfDatasetIsInUse(mdTxnCtx, dvId, dsId.getValue());

        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleDatasetDropStatement(metadataProvider, stmt, hcc, requestParameters);
    }

    @Override
    protected void handleIndexDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        String dvId = getActiveDataverse(((IndexDropStatement) stmt).getDataverseName());
        Identifier dsId = ((IndexDropStatement) stmt).getDatasetName();

        checkIfDatasetIsInUse(mdTxnCtx, dvId, dsId.getValue());

        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleIndexDropStatement(metadataProvider, stmt, hcc, requestParameters);
    }

    @Override
    protected void handleFunctionDropStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        FunctionSignature sig = ((FunctionDropStatement) stmt).getFunctionSignature();

        String dvId = getActiveDataverseName(sig.getNamespace());
        String function = sig.getName();
        String arity = Integer.toString(sig.getArity());

        List<Channel> channels = BADLangExtension.getAllChannels(mdTxnCtx);
        for (Channel channel : channels) {
            List<List<List<String>>> dependencies = channel.getDependencies();
            List<List<String>> datasetDependencies = dependencies.get(1);
            for (List<String> dependency : datasetDependencies) {
                if (dependency.get(0).equals(dvId) && dependency.get(1).equals(function)
                        && dependency.get(2).equals(arity)) {
                    throw new CompilationException(
                            "Cannot drop function " + sig + ". " + channel.getChannelId() + " depends on it!");
                }
            }

        }
        List<Procedure> procedures = BADLangExtension.getAllProcedures(mdTxnCtx);
        for (Procedure procedure : procedures) {
            List<List<List<String>>> dependencies = procedure.getDependencies();
            List<List<String>> datasetDependencies = dependencies.get(1);
            for (List<String> dependency : datasetDependencies) {
                if (dependency.get(0).equals(dvId) && dependency.get(1).equals(function)
                        && dependency.get(2).equals(arity)) {
                    throw new CompilationException(
                            "Cannot drop function " + sig + ". " + procedure.getEntityId() + " depends on it!");
                }
            }

        }

        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleFunctionDropStatement(metadataProvider, stmt);
    }

    @Override
    protected void handleDataverseDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        Identifier dvId = ((DataverseDropStatement) stmt).getDataverseName();
        MetadataProvider tempMdProvider = new MetadataProvider(appCtx, metadataProvider.getDefaultDataverse());
        tempMdProvider.getConfig().putAll(metadataProvider.getConfig());
        List<Channel> channels = BADLangExtension.getAllChannels(mdTxnCtx);
        for (Channel channel : channels) {
            if (channel.getChannelId().getDataverse().equals(dvId.getValue())) {
                continue;
            }
            List<List<List<String>>> dependencies = channel.getDependencies();
            for (List<List<String>> dependencyList : dependencies) {
                for (List<String> dependency : dependencyList) {
                    if (dependency.get(0).equals(dvId.getValue())) {
                        throw new CompilationException("Cannot drop dataverse " + dvId.getValue() + ". "
                                + channel.getChannelId() + " depends on it!");
                    }
                }
            }
        }
        List<Procedure> procedures = BADLangExtension.getAllProcedures(mdTxnCtx);
        for (Procedure procedure : procedures) {
            if (procedure.getEntityId().getDataverse().equals(dvId.getValue())) {
                continue;
            }
            List<List<List<String>>> dependencies = procedure.getDependencies();
            for (List<List<String>> dependencyList : dependencies) {
                for (List<String> dependency : dependencyList) {
                    if (dependency.get(0).equals(dvId.getValue())) {
                        throw new CompilationException("Cannot drop dataverse " + dvId.getValue() + ". "
                                + procedure.getEntityId() + " depends on it!");
                    }
                }
            }
        }
        final IRequestParameters requestParameters = new RequestParameters(null, null, null, null, null, null);
        for (Channel channel : channels) {
            if (!channel.getChannelId().getDataverse().equals(dvId.getValue())) {
                continue;
            }
            tempMdProvider.getLocks().reset();
            ChannelDropStatement drop =
                    new ChannelDropStatement(dvId, new Identifier(channel.getChannelId().getEntityName()), false);
            drop.handle(hcc, this, requestParameters, tempMdProvider, 0);
        }
        for (Procedure procedure : procedures) {
            if (!procedure.getEntityId().getDataverse().equals(dvId.getValue())) {
                continue;
            }
            tempMdProvider.getLocks().reset();
            ProcedureDropStatement drop = new ProcedureDropStatement(new FunctionSignature(dvId.getValue(),
                    procedure.getEntityId().getEntityName(), procedure.getArity()), false);
            drop.handle(hcc, this, requestParameters, tempMdProvider, 0);
        }
        List<Broker> brokers = BADLangExtension.getBrokers(mdTxnCtx, dvId.getValue());
        for (Broker broker : brokers) {
            tempMdProvider.getLocks().reset();
            BrokerDropStatement drop = new BrokerDropStatement(dvId, new Identifier(broker.getBrokerName()), false);
            drop.handle(hcc, this, requestParameters, tempMdProvider, 0);
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleDataverseDropStatement(metadataProvider, stmt, hcc);
    }

}
