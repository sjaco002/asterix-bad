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

import org.apache.asterix.active.ActiveJobNotificationHandler;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.algebra.extension.IExtensionStatement;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.metadata.PrecompiledJobEventListener;
import org.apache.asterix.bad.metadata.Procedure;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
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
import org.apache.hyracks.api.job.JobId;

public class ProcedureDropStatement implements IExtensionStatement {

    private final FunctionSignature signature;
    private boolean ifExists;

    public ProcedureDropStatement(FunctionSignature signature, boolean ifExists) {
        this.signature = signature;
        this.ifExists = ifExists;
    }

    public FunctionSignature getFunctionSignature() {
        return signature;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    @Override
    public byte getKind() {
        return Kind.EXTENSION;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

    @Override
    public void handle(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery, Stats stats,
            int resultSetIdCounter) throws HyracksDataException, AlgebricksException {
        FunctionSignature signature = getFunctionSignature();
        String dataverse =
                ((QueryTranslator) statementExecutor).getActiveDataverse(new Identifier(signature.getNamespace()));
        signature.setNamespace(dataverse);

        boolean txnActive = false;
        EntityId entityId = new EntityId(BADConstants.PROCEDURE_KEYWORD, dataverse, signature.getName());
        PrecompiledJobEventListener listener = (PrecompiledJobEventListener) ActiveJobNotificationHandler.INSTANCE
                .getActiveEntityListener(entityId);
        Procedure procedure = null;

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            txnActive = true;
            procedure = BADLangExtension.getProcedure(mdTxnCtx, dataverse, signature.getName(),
                    Integer.toString(signature.getArity()));
            txnActive = false;
            if (procedure == null) {
                if (ifExists) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("There is no procedure with this name " + signature.getName() + ".");
                }
            }

            if (listener.getExecutorService() != null) {
                listener.getExecutorService().shutdownNow();
            }
            JobId hyracksJobId = listener.getJobId();
            listener.deActivate();
            ActiveJobNotificationHandler.INSTANCE.removeJob(hyracksJobId, listener);
            if (hyracksJobId != null) {
                hcc.destroyJob(hyracksJobId);
            }

            //Remove the Channel Metadata
            MetadataManager.INSTANCE.deleteEntity(mdTxnCtx, procedure);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            e.printStackTrace();
            if (txnActive) {
                QueryTranslator.abort(e, e, mdTxnCtx);
            }
            throw new HyracksDataException(e);
        }
    }

}