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

import org.apache.asterix.active.EntityId;
import org.apache.asterix.algebra.extension.IExtensionStatement;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.bad.metadata.PrecompiledJobEventListener;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;

public class ChannelDropStatement implements IExtensionStatement {

    private final Identifier dataverseName;
    private final Identifier channelName;
    private boolean ifExists;

    public ChannelDropStatement(Identifier dataverseName, Identifier channelName, boolean ifExists) {
        this.dataverseName = dataverseName;
        this.channelName = channelName;
        this.ifExists = ifExists;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getChannelName() {
        return channelName;
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
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId)
            throws HyracksDataException, AlgebricksException {
        String dataverse = ((QueryTranslator) statementExecutor).getActiveDataverse(dataverseName);
        boolean txnActive = false;
        EntityId entityId = new EntityId(BADConstants.CHANNEL_EXTENSION_NAME, dataverse, channelName.getValue());
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        PrecompiledJobEventListener listener = (PrecompiledJobEventListener) activeEventHandler.getListener(entityId);
        Channel channel = null;

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            txnActive = true;
            channel = BADLangExtension.getChannel(mdTxnCtx, dataverse, channelName.getValue());
            txnActive = false;
            if (channel == null) {
                if (ifExists) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("There is no channel with this name " + channelName + ".");
                }
            }

            listener.getExecutorService().shutdownNow();
            JobId hyracksJobId = listener.getJobId();
            listener.deActivate();
            activeEventHandler.unregisterListener(listener);
            if (hyracksJobId != null) {
                hcc.destroyJob(hyracksJobId);
                // wait for job completion to release any resources to be dropped
                ensureJobDestroyed(hcc, hyracksJobId);
            }

            //Create a metadata provider to use in nested jobs.
            MetadataProvider tempMdProvider = new MetadataProvider(appCtx, metadataProvider.getDefaultDataverse());
            tempMdProvider.getConfig().putAll(metadataProvider.getConfig());
            //Drop the Channel Datasets
            //TODO: Need to find some way to handle if this fails.
            //TODO: Prevent datasets for Channels from being dropped elsewhere
            DropDatasetStatement dropStmt = new DropDatasetStatement(new Identifier(dataverse),
                    new Identifier(channel.getResultsDatasetName()), true);
            ((QueryTranslator) statementExecutor).handleDatasetDropStatement(tempMdProvider, dropStmt, hcc, null);
            tempMdProvider.getLocks().reset();
            dropStmt = new DropDatasetStatement(new Identifier(dataverse),
                    new Identifier(channel.getSubscriptionsDataset()), true);
            ((QueryTranslator) statementExecutor).handleDatasetDropStatement(tempMdProvider, dropStmt, hcc, null);

            //Remove the Channel Metadata
            MetadataManager.INSTANCE.deleteEntity(mdTxnCtx, channel);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            e.printStackTrace();
            if (txnActive) {
                QueryTranslator.abort(e, e, mdTxnCtx);
            }
            throw new HyracksDataException(e);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    private void ensureJobDestroyed(IHyracksClientConnection hcc, JobId hyracksJobId) throws Exception {
        try {
            hcc.waitForCompletion(hyracksJobId);
        } catch (Exception e) {
            // if the job has already been destroyed, it is safe to complete
            if (e instanceof HyracksDataException) {
                HyracksDataException hde = (HyracksDataException) e;
                if (hde.getComponent().equals(ErrorCode.HYRACKS)
                        && hde.getErrorCode() == ErrorCode.JOB_HAS_BEEN_CLEARED_FROM_HISTORY) {
                    return;
                }
            }
            throw e;
        }
    }
}