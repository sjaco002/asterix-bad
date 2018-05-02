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
package org.apache.asterix.bad.recovery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.bad.BADJobService;
import org.apache.asterix.bad.lang.BADCompilationProvider;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.lang.BADStatementExecutor;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener.InstanceChange;
import org.apache.asterix.bad.metadata.DeployedJobSpecEventListener.PrecompiledType;
import org.apache.asterix.bad.metadata.Procedure;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.hyracks.bootstrap.GlobalRecoveryManager;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.DeployedJobSpecIdFactory;
import org.apache.hyracks.control.common.utils.HyracksThreadFactory;

public class BADGlobalRecoveryManager extends GlobalRecoveryManager {

    private static final Logger LOGGER = Logger.getLogger(BADGlobalRecoveryManager.class.getName());


    @Override
    protected void recover(ICcApplicationContext appCtx) throws HyracksDataException {
        try {
            LOGGER.info("Starting Global Recovery");
            MetadataManager.INSTANCE.init();
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            mdTxnCtx = doRecovery(appCtx, mdTxnCtx);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            deployJobs(appCtx);
            recoveryCompleted = true;
            recovering = false;
            LOGGER.info("Global Recovery Completed. Refreshing cluster state...");
            appCtx.getClusterStateManager().refreshState();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private void deployJobs(ICcApplicationContext appCtx) throws Exception {

        MetadataProvider metadataProvider = new MetadataProvider(appCtx, MetadataBuiltinEntities.DEFAULT_DATAVERSE);
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        List<Channel> channels = BADLangExtension.getAllChannels(mdTxnCtx);
        List<Procedure> procedures = BADLangExtension.getAllProcedures(mdTxnCtx);

        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        metadataProvider.getLocks().unlock();

        SessionConfig sessionConfig =
                new SessionConfig(SessionConfig.OutputFormat.ADM, true, true, true, SessionConfig.PlanFormat.STRING);

        BADStatementExecutor badStatementExecutor = new BADStatementExecutor(appCtx, new ArrayList<>(),
                new SessionOutput(sessionConfig, null), new BADCompilationProvider(), Executors.newSingleThreadExecutor(
                        new HyracksThreadFactory(DefaultStatementExecutorFactory.class.getSimpleName())));

        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();

        //Remove any lingering listeners
        for (IActiveEntityEventsListener listener : activeEventHandler.getEventListeners()) {
            if (listener instanceof DeployedJobSpecEventListener) {
                activeEventHandler.unregisterListener(listener);
            }
        }

        DeployedJobSpecIdFactory deployedJobSpecIdFactory = new DeployedJobSpecIdFactory();

        //Redeploy Jobs
        for (Channel channel : channels) {
            EntityId entityId = channel.getChannelId();
            DeployedJobSpecId deployedJobSpecId = deployedJobSpecIdFactory.create();
            metadataProvider = new MetadataProvider(appCtx, MetadataBuiltinEntities.DEFAULT_DATAVERSE);
            DeployedJobSpecEventListener listener = new DeployedJobSpecEventListener(appCtx, entityId,
                    channel.getResultsDatasetName().equals("") ? PrecompiledType.PUSH_CHANNEL
                            : PrecompiledType.CHANNEL);
            listener.setInstanceCount(InstanceChange.LOCK);
            listener.storeDistributedInfo(deployedJobSpecId, null);
            activeEventHandler.registerListener(listener);
            BADJobService.redeployJobSpec(entityId, channel.getBody(), metadataProvider, badStatementExecutor, hcc,
                    new RequestParameters(null, null, null, null, null, null));

            ScheduledExecutorService ses = BADJobService.startRepetitiveDeployedJobSpec(deployedJobSpecId, hcc,
                    BADJobService.findPeriod(channel.getDuration()), new HashMap<>(), entityId,
                    metadataProvider.getTxnIdFactory(), listener);
            listener.storeDistributedInfo(deployedJobSpecId, ses);
            LOGGER.log(Level.SEVERE, entityId.getExtensionName() + " " + entityId.getDataverse() + "."
                    + entityId.getEntityName()
                    + " was stopped by cluster failure. It has restarted.");

        }
        for (Procedure procedure : procedures) {
            EntityId entityId = procedure.getEntityId();
            DeployedJobSpecId deployedJobSpecId = deployedJobSpecIdFactory.create();
            metadataProvider = new MetadataProvider(appCtx, MetadataBuiltinEntities.DEFAULT_DATAVERSE);
            DeployedJobSpecEventListener listener =
                    new DeployedJobSpecEventListener(appCtx, entityId, PrecompiledType.valueOf(procedure.getType()));
            listener.setInstanceCount(InstanceChange.LOCK);
            listener.storeDistributedInfo(deployedJobSpecId, null);
            activeEventHandler.registerListener(listener);
            BADJobService.redeployJobSpec(entityId, procedure.getBody(), metadataProvider, badStatementExecutor, hcc,
                    new RequestParameters(null, null, null, null, null, null));
            //Log that the procedure stopped by cluster restart. Procedure is available again now.
            LOGGER.log(Level.SEVERE, entityId.getExtensionName() + " " + entityId.getDataverse() + "."
                    + entityId.getEntityName()
                    + " was lost with cluster failure and any repetitive instances have stopped. It is now available to run again.");
            //TODO: allow repetitive procedures to restart execution automatically
            //Need to add "stop repetitive procedure" as well.
        }
        hcc.resetDeployedJobIdFactory(deployedJobSpecIdFactory.maxDeployedJobSpecId());
    }
}
