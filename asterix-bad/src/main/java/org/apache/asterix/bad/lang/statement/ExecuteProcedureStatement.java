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

import java.util.EnumSet;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.asterix.active.ActiveJobNotificationHandler;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.algebra.extension.IExtensionStatement;
import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.ChannelJobService;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.metadata.PrecompiledJobEventListener;
import org.apache.asterix.bad.metadata.PrecompiledJobEventListener.PrecompiledType;
import org.apache.asterix.bad.metadata.Procedure;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;

public class ExecuteProcedureStatement implements IExtensionStatement {

    private final String dataverseName;
    private final String procedureName;
    private final int arity;

    public ExecuteProcedureStatement(String dataverseName, String procedureName, int arity) {
        this.dataverseName = dataverseName;
        this.procedureName = procedureName;
        this.arity = arity;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getProcedureName() {
        return procedureName;
    }

    public int getArity() {
        return arity;
    }

    @Override
    public byte getKind() {
        return Kind.EXTENSION;
    }

    @Override
    public byte getCategory() {
        return Category.UPDATE;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

    @Override
    public void handle(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery, Stats stats,
            int resultSetIdCounter) throws HyracksDataException, AlgebricksException {


        String dataverse = ((QueryTranslator) statementExecutor).getActiveDataverse(new Identifier(dataverseName));
        boolean txnActive = false;
        EntityId entityId = new EntityId(BADConstants.PROCEDURE_KEYWORD, dataverse, procedureName);
        PrecompiledJobEventListener listener = (PrecompiledJobEventListener) ActiveJobNotificationHandler.INSTANCE
                .getActiveEntityListener(entityId);
        Procedure procedure = null;

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            txnActive = true;
            procedure = BADLangExtension.getProcedure(mdTxnCtx, dataverse, procedureName,
                    Integer.toString(getArity()));
            if (procedure == null) {
                throw new AlgebricksException("There is no procedure with this name " + procedureName + ".");
            }

            JobId hyracksJobId = listener.getJobId();
            if (procedure.getDuration().equals("")) {
                hcc.startJob(hyracksJobId);

                if (listener.getType() == PrecompiledType.QUERY) {
                    hcc.waitForCompletion(hyracksJobId);
                    ResultReader resultReader = listener.getResultReader();
                    ResultUtil.printResults(resultReader, ((QueryTranslator) statementExecutor).getSessionConfig(),
                            new Stats(), null);
                }

            } else {
                ScheduledExecutorService ses = ChannelJobService.startJob(null, EnumSet.noneOf(JobFlag.class),
                        hyracksJobId, hcc, ChannelJobService.findPeriod(procedure.getDuration()));
                listener.storeDistributedInfo(hyracksJobId, ses, listener.getResultReader());
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            txnActive = false;
        } catch (Exception e) {
            e.printStackTrace();
            if (txnActive) {
                QueryTranslator.abort(e, e, mdTxnCtx);
            }
            throw new HyracksDataException(e);
        }
    }

}