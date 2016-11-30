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

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.bad.metadata.Broker;
import org.apache.asterix.bad.metadata.BrokerSearchKey;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.bad.metadata.ChannelSearchKey;
import org.apache.asterix.bad.metadata.DataverseBrokersSearchKey;
import org.apache.asterix.bad.metadata.DataverseChannelsSearchKey;
import org.apache.asterix.bad.metadata.Procedure;
import org.apache.asterix.bad.metadata.ProcedureSearchKey;
import org.apache.asterix.common.api.ExtensionId;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class BADLangExtension implements ILangExtension {

    public static final ExtensionId EXTENSION_ID = new ExtensionId(BADLangExtension.class.getSimpleName(), 0);

    @Override
    public ExtensionId getId() {
        return EXTENSION_ID;
    }

    @Override
    public void configure(List<Pair<String, String>> args) {
    }

    @Override
    public ILangCompilationProvider getLangCompilationProvider(Language lang) {
        switch (lang) {
            case AQL:
                return new BADCompilationProvider();
            case SQLPP:
                return new SqlppCompilationProvider();
            default:
                return null;
        }
    }

    @Override
    public ExtensionKind getExtensionKind() {
        return ExtensionKind.LANG;
    }


    public static Broker getBroker(MetadataTransactionContext mdTxnCtx, String dataverseName, String brokerName)
            throws AlgebricksException {
        BrokerSearchKey brokerSearchKey = new BrokerSearchKey(dataverseName, brokerName);
        List<Broker> brokers = MetadataManager.INSTANCE.getEntities(mdTxnCtx, brokerSearchKey);
        if (brokers.isEmpty()) {
            return null;
        } else if (brokers.size() > 1) {
            throw new AlgebricksException("Broker search key returned more than one broker");
        } else {
            return brokers.get(0);
        }
    }

    public static Channel getChannel(MetadataTransactionContext mdTxnCtx, String dataverseName, String channelName)
            throws AlgebricksException {
        ChannelSearchKey channelSearchKey = new ChannelSearchKey(dataverseName, channelName);
        List<Channel> channels = MetadataManager.INSTANCE.getEntities(mdTxnCtx, channelSearchKey);
        if (channels.isEmpty()) {
            return null;
        } else if (channels.size() > 1) {
            throw new AlgebricksException("Channel search key returned more than one channel");
        } else {
            return channels.get(0);
        }
    }

    public static Procedure getProcedure(MetadataTransactionContext mdTxnCtx, String dataverseName,
            String procedureName, String arity) throws AlgebricksException {
        ProcedureSearchKey procedureSearchKey = new ProcedureSearchKey(dataverseName, procedureName, arity);
        List<Procedure> procedures = MetadataManager.INSTANCE.getEntities(mdTxnCtx, procedureSearchKey);
        if (procedures.isEmpty()) {
            return null;
        } else if (procedures.size() > 1) {
            throw new AlgebricksException("Procedure search key returned more than one channel");
        } else {
            return procedures.get(0);
        }
    }

    public static List<Broker> getBrokers(MetadataTransactionContext mdTxnCtx, String dataverseName)
            throws AlgebricksException {
        DataverseBrokersSearchKey brokerSearchKey = new DataverseBrokersSearchKey(dataverseName);
        return MetadataManager.INSTANCE.getEntities(mdTxnCtx, brokerSearchKey);
    }

    public static List<Channel> getChannels(MetadataTransactionContext mdTxnCtx, String dataverseName)
            throws AlgebricksException {
        DataverseChannelsSearchKey channelSearchKey = new DataverseChannelsSearchKey(dataverseName);
        return MetadataManager.INSTANCE.getEntities(mdTxnCtx, channelSearchKey);
    }

}
