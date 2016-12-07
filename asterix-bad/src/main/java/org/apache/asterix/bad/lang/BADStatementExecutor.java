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

import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.lang.statement.BrokerDropStatement;
import org.apache.asterix.bad.lang.statement.ChannelDropStatement;
import org.apache.asterix.bad.metadata.Broker;
import org.apache.asterix.bad.metadata.Channel;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.SessionConfig;
import org.apache.hyracks.api.client.IHyracksClientConnection;

public class BADStatementExecutor extends QueryTranslator {

    public BADStatementExecutor(List<Statement> aqlStatements, SessionConfig conf,
            ILangCompilationProvider compliationProvider) {
        super(aqlStatements, conf, compliationProvider);
    }


    @Override
    protected void handleDataverseDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        //TODO: Remove this when metadata dependencies are in place
        //TODO: Stop dataset drop when dataset used by channel
        super.handleDataverseDropStatement(metadataProvider, stmt, hcc);
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        Identifier dvId = ((DataverseDropStatement) stmt).getDataverseName();
        List<Broker> brokers = BADLangExtension.getBrokers(mdTxnCtx, dvId.getValue());
        for (Broker broker : brokers) {
            BrokerDropStatement drop = new BrokerDropStatement(dvId, new Identifier(broker.getBrokerName()), false);
            drop.handle(this, metadataProvider, hcc, null, null, null, 0);
        }
        List<Channel> channels = BADLangExtension.getChannels(mdTxnCtx, dvId.getValue());
        for (Channel channel : channels) {
            ChannelDropStatement drop = new ChannelDropStatement(dvId,
                    new Identifier(channel.getChannelId().getEntityName()), false);
            drop.handle(this, metadataProvider, hcc, null, null, null, 0);
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    }

}
