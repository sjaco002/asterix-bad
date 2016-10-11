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
package org.apache.asterix.bad;

public interface BADConstants {
    final String SubscriptionId = "subscriptionId";
    final String BrokerName = "BrokerName";
    final String ChannelName = "ChannelName";
    final String DataverseName = "DataverseName";
    final String BrokerEndPoint = "BrokerEndPoint";
    final String DeliveryTime = "deliveryTime";
    final String ResultId = "resultId";
    final String ChannelExecutionTime = "channelExecutionTime";
    final String ChannelSubscriptionsType = "ChannelSubscriptionsType";
    final String ChannelResultsType = "ChannelResultsType";
    final String ResultsDatasetName = "ResultsDatasetName";
    final String SubscriptionsDatasetName = "SubscriptionsDatasetName";
    final String CHANNEL_EXTENSION_NAME = "Channel";
    final String BROKER_KEYWORD = "Broker";
    final String RECORD_TYPENAME_BROKER = "BrokerRecordType";
    final String RECORD_TYPENAME_CHANNEL = "ChannelRecordType";
    final String subscriptionEnding = "Subscriptions";
    final String resultsEnding = "Results";
    final String BAD_METADATA_EXTENSION_NAME = "BADMetadataExtension";
    final String BAD_DATAVERSE_NAME = "Metadata";
    final String Duration = "Duration";
    final String Function = "Function";

    public enum ChannelJobType {
        REPETITIVE
    }
}
