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

import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.active.ActiveJobNotificationHandler;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.message.ActiveManagerMessage;
import org.apache.asterix.algebra.extension.IExtensionStatement;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.ChannelJobInfo;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.bad.metadata.ChannelEventsListener;
import org.apache.asterix.bad.runtime.RepetitiveChannelOperatorNodePushable;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.external.feed.api.IActiveLifecycleEventSubscriber;
import org.apache.asterix.external.feed.api.IActiveLifecycleEventSubscriber.ActiveLifecycleEvent;
import org.apache.asterix.external.feed.management.ActiveLifecycleEventSubscriber;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.runtime.util.AppContextInfo;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.exceptions.HyracksDataException;

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
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return null;
    }

    @Override
    public void handle(IStatementExecutor statementExecutor, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery, Stats stats,
            int resultSetIdCounter) throws HyracksDataException, AlgebricksException {

        String dataverse = ((QueryTranslator) statementExecutor).getActiveDataverse(dataverseName);
        boolean txnActive = false;
        EntityId entityId = new EntityId(BADConstants.CHANNEL_EXTENSION_NAME, dataverse, channelName.getValue());
        ChannelEventsListener listener = (ChannelEventsListener) ActiveJobNotificationHandler.INSTANCE
                .getActiveEntityListener(entityId);
        IActiveLifecycleEventSubscriber eventSubscriber = new ActiveLifecycleEventSubscriber();
        boolean subscriberRegistered = false;
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
            if (listener != null) {
                subscriberRegistered = listener.isChannelActive(entityId, eventSubscriber);
            }
            if (!subscriberRegistered) {
                throw new AsterixException("Channel " + channelName + " is not running");
            }

            ICCMessageBroker messageBroker =
                    (ICCMessageBroker) AppContextInfo.INSTANCE.getCCApplicationContext().getMessageBroker();

            ChannelJobInfo cInfo = listener.getJobInfo(channel.getChannelId());;
            Set<String> ncs = new HashSet<>(cInfo.getLocations());
            AlgebricksAbsolutePartitionConstraint locations = new AlgebricksAbsolutePartitionConstraint(
                    ncs.toArray(new String[ncs.size()]));
            int partition = 0;
            for (String location : locations.getLocations()) {
                messageBroker.sendApplicationMessageToNC(
                        new ActiveManagerMessage(ActiveManagerMessage.STOP_ACTIVITY, "cc",
                                new ActiveRuntimeId(channel.getChannelId(),
                                        RepetitiveChannelOperatorNodePushable.class.getSimpleName(), partition++)),
                        location);
            }
            eventSubscriber.assertEvent(ActiveLifecycleEvent.ACTIVE_JOB_ENDED);

            //Drop the Channel Datasets
            //TODO: Need to find some way to handle if this fails.
            //TODO: Prevent datasets for Channels from being dropped elsewhere
            DropDatasetStatement dropStmt = new DropDatasetStatement(new Identifier(dataverse),
                    new Identifier(channel.getResultsDatasetName()), true);
            ((QueryTranslator) statementExecutor).handleDatasetDropStatement(metadataProvider, dropStmt, hcc);

            dropStmt = new DropDatasetStatement(new Identifier(dataverse),
                    new Identifier(channel.getSubscriptionsDataset()), true);
            ((QueryTranslator) statementExecutor).handleDatasetDropStatement(metadataProvider, dropStmt, hcc);

            if (subscriberRegistered) {
                listener.deregisterEventSubscriber(eventSubscriber);
            }

            //Remove the Channel Metadata
            MetadataManager.INSTANCE.deleteEntity(mdTxnCtx, channel);
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