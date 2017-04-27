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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveJobNotificationHandler;
import org.apache.asterix.active.ActiveLifecycleListener;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.algebra.extension.IExtensionStatement;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.metadata.PrecompiledJobEventListener;
import org.apache.asterix.bad.metadata.PrecompiledJobEventListener.PrecompiledType;
import org.apache.asterix.bad.metadata.Procedure;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RuntimeContextVarExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.SqlppDeleteRewriteVisitor;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.base.temporal.ADurationParserFactory;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
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
    private final String procedureBody;
    private final Statement procedureBodyStatement;
    private final List<String> paramList;
    private final List<VariableExpr> varList;
    private final CallExpr period;
    private String duration = "";

    public CreateProcedureStatement(FunctionSignature signature, List<VarIdentifier> parameterList,
            List<Integer> paramIds, String functionBody, Statement procedureBodyStatement, Expression period) {
        this.signature = signature;
        this.procedureBody = functionBody;
        this.procedureBodyStatement = procedureBodyStatement;
        this.paramList = new ArrayList<>();
        this.varList = new ArrayList<>();
        for (int i = 0; i < parameterList.size(); i++) {
            this.paramList.add(parameterList.get(i).getValue());
            this.varList.add(new VariableExpr(new VarIdentifier(parameterList.get(i).toString(), paramIds.get(i))));
        }
        this.period = (CallExpr) period;
    }

    public String getProcedureBody() {
        return procedureBody;
    }

    public Statement getProcedureBodyStatement() {
        return procedureBodyStatement;
    }

    @Override
    public byte getKind() {
        return Kind.EXTENSION;
    }

    public List<String> getParamList() {
        return paramList;
    }

    public List<VariableExpr> getVarList() {
        return varList;
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

    private JobSpecification compileQueryJob(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, Query q) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        JobSpecification jobSpec = null;
        try {
            jobSpec = ((QueryTranslator) statementExecutor).rewriteCompileQuery(hcc, metadataProvider, q, null);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
        } catch (Exception e) {
            LOGGER.log(Level.INFO, e.getMessage(), e);
            if (bActiveTxn) {
                ((QueryTranslator) statementExecutor).abort(e, e, mdTxnCtx);
            }
            throw e;
        }
        return jobSpec;
    }

    private Pair<JobSpecification, PrecompiledType> createProcedureJob(IStatementExecutor statementExecutor,
            MetadataProvider metadataProvider, IHyracksClientConnection hcc, IHyracksDataset hdc, Stats stats)
                    throws Exception {
        if (getProcedureBodyStatement().getKind() == Statement.Kind.INSERT) {
            return new Pair<>(
                    ((QueryTranslator) statementExecutor).handleInsertUpsertStatement(metadataProvider,
                            getProcedureBodyStatement(), hcc, hdc, ResultDelivery.ASYNC, stats, true, null, null),
                    PrecompiledType.INSERT);
        } else if (getProcedureBodyStatement().getKind() == Statement.Kind.QUERY) {
            Pair<JobSpecification, PrecompiledType> pair = new Pair<>(
                    compileQueryJob(statementExecutor, metadataProvider, hcc, (Query) getProcedureBodyStatement()),
                    PrecompiledType.QUERY);
            metadataProvider.getLocks().unlock();
            return pair;
        } else if (getProcedureBodyStatement().getKind() == Statement.Kind.DELETE) {
            SqlppDeleteRewriteVisitor visitor = new SqlppDeleteRewriteVisitor();
            getProcedureBodyStatement().accept(visitor, null);
            DeleteStatement delete = (DeleteStatement) getProcedureBodyStatement();
            FLWOGRExpression flwogr = (FLWOGRExpression)delete.getQuery().getBody();
            for (VariableExpr var : varList){
                //Expression con = new LiteralExpr(new IntegerLiteral(var.getVar().getId()));
                Expression con = new RuntimeContextVarExpr(var.getVar().getValue());
                LetClause let = new LetClause(var, con);
                flwogr.getClauseList().add(0, let);
            }
            return new Pair<>(((QueryTranslator) statementExecutor).handleDeleteStatement(metadataProvider,
                    getProcedureBodyStatement(), hcc, true), PrecompiledType.DELETE);
        } else {
            throw new CompilationException("Procedure can only execute a single delete, insert, or query");
        }
    }

    private void setupDistributedJob(EntityId entityId, JobSpecification jobSpec, IHyracksClientConnection hcc,
            PrecompiledJobEventListener listener, ResultSetId resultSetId, IHyracksDataset hdc, Stats stats)
            throws Exception {
        JobId jobId = hcc.distributeJob(jobSpec);
        listener.storeDistributedInfo(jobId, null, new ResultReader(hdc, jobId, resultSetId));
    }

    @Override
    public void handle(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery, Stats stats,
            int resultSetIdCounter) throws HyracksDataException, AlgebricksException {
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        ActiveLifecycleListener activeListener = (ActiveLifecycleListener) appCtx.getActiveLifecycleListener();
        ActiveJobNotificationHandler activeEventHandler = activeListener.getNotificationHandler();
        initialize();
        String dataverse =
                ((QueryTranslator) statementExecutor).getActiveDataverse(new Identifier(signature.getNamespace()));
        EntityId entityId = new EntityId(BADConstants.PROCEDURE_KEYWORD, dataverse, signature.getName());
        PrecompiledJobEventListener listener =
                (PrecompiledJobEventListener) activeEventHandler.getActiveEntityListener(entityId);
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
                    Function.RETURNTYPE_VOID, getProcedureBody(), Function.LANGUAGE_AQL, duration);

            MetadataProvider tempMdProvider = new MetadataProvider(metadataProvider.getApplicationContext(),
                    metadataProvider.getDefaultDataverse(), metadataProvider.getStorageComponentProvider());
            tempMdProvider.setConfig(metadataProvider.getConfig());

            metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter++));
            boolean resultsAsync = resultDelivery == ResultDelivery.ASYNC || resultDelivery == ResultDelivery.DEFERRED;
            metadataProvider.setResultAsyncMode(resultsAsync);
            tempMdProvider.setResultSetId(metadataProvider.getResultSetId());
            tempMdProvider.setResultAsyncMode(resultsAsync);
            tempMdProvider.setWriterFactory(metadataProvider.getWriterFactory());
            tempMdProvider.setResultSerializerFactoryProvider(metadataProvider.getResultSerializerFactoryProvider());
            tempMdProvider.setOutputFile(metadataProvider.getOutputFile());

            //Create Procedure Internal Job
            Pair<JobSpecification, PrecompiledType> procedureJobSpec =
                    createProcedureJob(statementExecutor, tempMdProvider, hcc, hdc, stats);

            // Now we subscribe
            if (listener == null) {
                //TODO: Add datasets used by channel function
                listener = new PrecompiledJobEventListener(entityId, procedureJobSpec.second, new ArrayList<>());
                activeEventHandler.registerListener(listener);
            }
            setupDistributedJob(entityId, procedureJobSpec.first, hcc, listener, tempMdProvider.getResultSetId(), hdc,
                    stats);

            MetadataManager.INSTANCE.addEntity(mdTxnCtx, procedure);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (mdTxnCtx != null) {
                QueryTranslator.abort(e, e, mdTxnCtx);
            }
            LOGGER.log(Level.WARNING, "Failed creating a procedure", e);
            throw new HyracksDataException(e);
        } finally {
            metadataProvider.getLocks().unlock();
        }

    }

}
