/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.bad.lang.statement;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveJobNotificationHandler;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.algebra.extension.IExtensionStatement;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.metadata.ChannelEventsListener;
import org.apache.asterix.bad.metadata.Procedure;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.external.feed.api.IActiveLifecycleEventSubscriber;
import org.apache.asterix.external.feed.api.IActiveLifecycleEventSubscriber.ActiveLifecycleEvent;
import org.apache.asterix.external.feed.management.ActiveLifecycleEventSubscriber;
import org.apache.asterix.lang.aql.parser.AQLParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;

public class CreateProcedureStatement implements IExtensionStatement {

    private static final Logger LOGGER = Logger.getLogger(CreateProcedureStatement.class.getName());

    private final FunctionSignature signature;
    private final String functionBody;
    private final List<String> paramList;

    public FunctionSignature getaAterixFunction() {
        return signature;
    }

    public String getFunctionBody() {
        return functionBody;
    }

    public CreateProcedureStatement(FunctionSignature signature, List<VarIdentifier> parameterList,
            String functionBody) {
        this.signature = signature;
        this.functionBody = functionBody;
        this.paramList = new ArrayList<String>();
        for (VarIdentifier varId : parameterList) {
            this.paramList.add(varId.getValue());
        }
    }

    @Override
    public byte getKind() {
        return Kind.EXTENSION;
    }

    public List<String> getParamList() {
        return paramList;
    }

    public FunctionSignature getSignature() {
        return signature;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return null;
    }

    private JobSpecification createProcedureJob(String body, IStatementExecutor statementExecutor,
            MetadataProvider metadataProvider, IHyracksClientConnection hcc, IHyracksDataset hdc, Stats stats)
                    throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append(body);
        builder.append(";");
        AQLParserFactory aqlFact = new AQLParserFactory();
        List<Statement> fStatements = aqlFact.createParser(new StringReader(builder.toString())).parse();
        if (fStatements.size() > 1) {
            throw new Exception("Procedure can only execute a single statement");
        }
        return ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider, fStatements.get(0),
                hcc, hdc, ResultDelivery.ASYNC, stats, true);
    }

    @Override
    public void handle(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery, Stats stats,
            int resultSetIdCounter) throws HyracksDataException, AlgebricksException {

        String dataverse =
                ((QueryTranslator) statementExecutor).getActiveDataverse(new Identifier(signature.getNamespace()));

        EntityId entityId = new EntityId(BADConstants.PROCEDURE_KEYWORD, dataverse, signature.getName());
        ChannelEventsListener listener =
                (ChannelEventsListener) ActiveJobNotificationHandler.INSTANCE.getActiveEntityListener(entityId);
        IActiveLifecycleEventSubscriber eventSubscriber = new ActiveLifecycleEventSubscriber();
        boolean subscriberRegistered = false;
        Procedure procedure = null;

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            procedure = BADLangExtension.getProcedure(mdTxnCtx, dataverse, signature.getName(),
                    Integer.toString(signature.getArity()));
            if (procedure != null) {
                throw new AlgebricksException("A procedure with this name " + signature.getName() + " already exists.");
            }
            if (listener != null) {
                subscriberRegistered = listener.isChannelActive(entityId, eventSubscriber);
            }
            if (subscriberRegistered) {
                throw new AsterixException("Procedure " + signature.getName() + " is already running");
            }

            procedure = new Procedure(dataverse, signature.getName(), signature.getArity(), getParamList(),
                    Function.RETURNTYPE_VOID, getFunctionBody(), Function.LANGUAGE_AQL);

            // Now we subscribe
            if (listener == null) {
                listener = new ChannelEventsListener(entityId);
                ActiveJobNotificationHandler.INSTANCE.registerListener(listener);
            }
            listener.registerEventSubscriber(eventSubscriber);
            subscriberRegistered = true;


            //Create Procedure Internal Job
            JobSpecification channeljobSpec =
                    createProcedureJob(getFunctionBody(), statementExecutor, metadataProvider, hcc, hdc, stats);

            // setupDistributedJob(entityId, channeljobSpec, hcc);

            eventSubscriber.assertEvent(ActiveLifecycleEvent.ACTIVE_JOB_STARTED);

            MetadataManager.INSTANCE.addEntity(mdTxnCtx, procedure);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (mdTxnCtx != null) {
                QueryTranslator.abort(e, e, mdTxnCtx);
            }
            LOGGER.log(Level.WARNING, "Failed creating a procedure", e);
            throw new HyracksDataException(e);
        }

    }

}
