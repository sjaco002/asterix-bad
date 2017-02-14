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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveJobNotificationHandler;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.algebra.extension.IExtensionStatement;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.DistributedJobInfo;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.metadata.PrecompiledJobEventListener;
import org.apache.asterix.bad.metadata.PrecompiledJobEventListener.PrecompiledType;
import org.apache.asterix.bad.metadata.Procedure;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.aql.parser.AQLParserFactory;
import org.apache.asterix.lang.aql.visitor.AqlDeleteRewriteVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.base.temporal.ADurationParserFactory;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.SessionConfig;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;

public class CreateProcedureStatement implements IExtensionStatement {

    private static final Logger LOGGER = Logger.getLogger(CreateProcedureStatement.class.getName());

    private final FunctionSignature signature;
    private final String functionBody;
    private final List<String> paramList;
    private final CallExpr period;
    private String duration = "";

    public FunctionSignature getaAterixFunction() {
        return signature;
    }

    public String getFunctionBody() {
        return functionBody;
    }

    public CreateProcedureStatement(FunctionSignature signature, List<VarIdentifier> parameterList, String functionBody,
            Expression period) {
        this.signature = signature;
        this.functionBody = functionBody;
        this.paramList = new ArrayList<String>();
        for (VarIdentifier varId : parameterList) {
            this.paramList.add(varId.getValue());
        }
        this.period = (CallExpr) period;
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

    public Expression getPeriod() {
        return period;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

    private void initialize() throws MetadataException, HyracksDataException {
        if (period == null) {
            return;
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

    private Pair<JobSpecification, PrecompiledType> createProcedureJob(String body,
            IStatementExecutor statementExecutor,
            MetadataProvider metadataProvider, IHyracksClientConnection hcc, IHyracksDataset hdc, Stats stats)
                    throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append(body);
        builder.append(";");
        AQLParserFactory aqlFact = new AQLParserFactory();
        List<Statement> fStatements = aqlFact.createParser(new StringReader(builder.toString())).parse();
        if (fStatements.size() > 1) {
            throw new CompilationException("Procedure can only execute a single statement");
        }
        if (fStatements.get(0).getKind() == Statement.Kind.INSERT) {
            return new Pair<>(((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider,
                    fStatements.get(0), hcc, hdc, ResultDelivery.ASYNC, stats, true), PrecompiledType.INSERT);
        } else if (fStatements.get(0).getKind() == Statement.Kind.QUERY) {
            return new Pair<>(((QueryTranslator) statementExecutor).rewriteCompileQuery(hcc, metadataProvider,
                    (Query) fStatements.get(0), null), PrecompiledType.QUERY);
        } else if (fStatements.get(0).getKind() == Statement.Kind.DELETE) {
            AqlDeleteRewriteVisitor visitor = new AqlDeleteRewriteVisitor();
            fStatements.get(0).accept(visitor, null);
            return new Pair<>(((QueryTranslator) statementExecutor).handleDeleteStatement(metadataProvider,
                    fStatements.get(0), hcc, true), PrecompiledType.DELETE);
        }else{
            throw new CompilationException("Procedure can only execute a single delete, insert, or query");
        }
    }

    private void setupDistributedJob(EntityId entityId, JobSpecification jobSpec, IHyracksClientConnection hcc,
            PrecompiledJobEventListener listener, MetadataProvider metadataProvider,
            IHyracksDataset hdc, SessionConfig sessionConfig, Stats stats) throws Exception {
        DistributedJobInfo distributedJobInfo = new DistributedJobInfo(entityId, null, ActivityState.ACTIVE, jobSpec);
        jobSpec.setProperty(ActiveJobNotificationHandler.ACTIVE_ENTITY_PROPERTY_NAME, distributedJobInfo);
        JobId jobId = hcc.distributeJob(jobSpec);
        listener.storeDistributedInfo(jobId, null, new ResultReader(hdc), metadataProvider.getResultSetId());
    }

    @Override
    public void handle(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery, Stats stats,
            int resultSetIdCounter) throws HyracksDataException, AlgebricksException {

        initialize();

        String dataverse =
                ((QueryTranslator) statementExecutor).getActiveDataverse(new Identifier(signature.getNamespace()));

        EntityId entityId = new EntityId(BADConstants.PROCEDURE_KEYWORD, dataverse, signature.getName());
        PrecompiledJobEventListener listener =
                (PrecompiledJobEventListener) ActiveJobNotificationHandler.INSTANCE.getActiveEntityListener(entityId);
        boolean alreadyActive = false;
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
                alreadyActive = listener.isEntityActive();
            }
            if (alreadyActive) {
                throw new AsterixException("Procedure " + signature.getName() + " is already running");
            }

            procedure = new Procedure(dataverse, signature.getName(), signature.getArity(), getParamList(),
                    Function.RETURNTYPE_VOID, getFunctionBody(), Function.LANGUAGE_AQL, duration);

            metadataProvider.setResultSetId(new ResultSetId(0));
            metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter++));

            //Create Procedure Internal Job
            Pair<JobSpecification, PrecompiledType> procedureJobSpec =
                    createProcedureJob(getFunctionBody(), statementExecutor, metadataProvider, hcc, hdc, stats);

            // Now we subscribe
            if (listener == null) {
                listener = new PrecompiledJobEventListener(entityId, procedureJobSpec.second);
                ActiveJobNotificationHandler.INSTANCE.registerListener(listener);
            }

            setupDistributedJob(entityId, procedureJobSpec.first, hcc, listener, metadataProvider,
                    hdc, ((QueryTranslator) statementExecutor).getSessionConfig(), stats);

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
