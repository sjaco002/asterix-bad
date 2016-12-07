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
    public static final String SubscriptionId = "subscriptionId";
    public static final String BrokerName = "BrokerName";
    public static final String ChannelName = "ChannelName";
    public static final String ProcedureName = "ProcedureName";
    public static final String DataverseName = "DataverseName";
    public static final String BrokerEndPoint = "BrokerEndPoint";
    public static final String DeliveryTime = "deliveryTime";
    public static final String ResultId = "resultId";
    public static final String ChannelExecutionTime = "channelExecutionTime";
    public static final String ChannelSubscriptionsType = "ChannelSubscriptionsType";
    public static final String ChannelResultsType = "ChannelResultsType";
    public static final String ResultsDatasetName = "ResultsDatasetName";
    public static final String SubscriptionsDatasetName = "SubscriptionsDatasetName";
    public static final String CHANNEL_EXTENSION_NAME = "Channel";
    public static final String PROCEDURE_KEYWORD = "Procedure";
    public static final String BROKER_KEYWORD = "Broker";
    public static final String RECORD_TYPENAME_BROKER = "BrokerRecordType";
    public static final String RECORD_TYPENAME_CHANNEL = "ChannelRecordType";
    public static final String RECORD_TYPENAME_PROCEDURE = "ProcedureRecordType";
    public static final String subscriptionEnding = "Subscriptions";
    public static final String resultsEnding = "Results";
    public static final String BAD_METADATA_EXTENSION_NAME = "BADMetadataExtension";
    public static final String BAD_DATAVERSE_NAME = "Metadata";
    public static final String Duration = "Duration";
    public static final String Function = "Function";
    public static final String FIELD_NAME_ARITY = "Arity";
    public static final String FIELD_NAME_PARAMS = "Params";
    public static final String FIELD_NAME_RETURN_TYPE = "ReturnType";
    public static final String FIELD_NAME_DEFINITION = "Definition";
    public static final String FIELD_NAME_LANGUAGE = "Language";

    public enum ChannelJobType {
        REPETITIVE
    }
}
