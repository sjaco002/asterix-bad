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
package org.apache.asterix.bad.metadata;

import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public class BADMetadataRecordTypes {

    // -------------------------------------- Subscriptions --------------------------------------//
    private static final String[] subTypeFieldNames = { BADConstants.DataverseName, BADConstants.BrokerName,
            BADConstants.SubscriptionId };
    private static final IAType[] subTypeFieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AUUID };
    public static final ARecordType channelSubscriptionsType = new ARecordType(BADConstants.ChannelSubscriptionsType,
            subTypeFieldNames, subTypeFieldTypes, true);

    // ---------------------------------------- Results --------------------------------------------//
    private static final String[] resultTypeFieldNames = { BADConstants.ResultId, BADConstants.ChannelExecutionTime,
            BADConstants.SubscriptionId, BADConstants.DeliveryTime };
    private static final IAType[] resultTypeFieldTypes = { BuiltinType.AUUID, BuiltinType.ADATETIME, BuiltinType.AUUID,
            BuiltinType.ADATETIME };
    public static final ARecordType channelResultsType = new ARecordType(BADConstants.ChannelResultsType,
            resultTypeFieldNames, resultTypeFieldTypes, true);

    //------------------------------------------ Channel ----------------------------------------//     
    public static final int CHANNEL_ARECORD_DATAVERSE_NAME_FIELD_INDEX = 0;
    public static final int CHANNEL_ARECORD_CHANNEL_NAME_FIELD_INDEX = 1;
    public static final int CHANNEL_ARECORD_SUBSCRIPTIONS_NAME_FIELD_INDEX = 2;
    public static final int CHANNEL_ARECORD_RESULTS_NAME_FIELD_INDEX = 3;
    public static final int CHANNEL_ARECORD_FUNCTION_FIELD_INDEX = 4;
    public static final int CHANNEL_ARECORD_DURATION_FIELD_INDEX = 5;
    public static final ARecordType CHANNEL_RECORDTYPE = MetadataRecordTypes.createRecordType(
            // RecordTypeName
            BADConstants.RECORD_TYPENAME_CHANNEL,
            // FieldNames
            new String[] { BADConstants.DataverseName, BADConstants.ChannelName, BADConstants.SubscriptionsDatasetName,
                    BADConstants.ResultsDatasetName, BADConstants.Function, BADConstants.Duration },
            // FieldTypes
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                    BuiltinType.ASTRING, BuiltinType.ASTRING },
            //IsOpen?
            true);
    //------------------------------------------ Broker ----------------------------------------//
    public static final int BROKER_DATAVERSE_NAME_FIELD_INDEX = 0;
    public static final int BROKER_NAME_FIELD_INDEX = 1;
    public static final int BROKER_ENDPOINT_FIELD_INDEX = 2;
    public static final ARecordType BROKER_RECORDTYPE = MetadataRecordTypes.createRecordType(
            // RecordTypeName
            BADConstants.RECORD_TYPENAME_BROKER,
            // FieldNames
            new String[] { BADConstants.DataverseName, BADConstants.BrokerName, BADConstants.BrokerEndPoint },
            // FieldTypes
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                    BuiltinType.ASTRING, BuiltinType.ASTRING },
            //IsOpen?
            true);

    //----------------------------------------- Procedure ----------------------------------------//
    public static final int PROCEDURE_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int PROCEDURE_ARECORD_PROCEDURE_NAME_FIELD_INDEX = 1;
    public static final int PROCEDURE_ARECORD_PROCEDURE_ARITY_FIELD_INDEX = 2;
    public static final int PROCEDURE_ARECORD_PROCEDURE_PARAM_LIST_FIELD_INDEX = 3;
    public static final int PROCEDURE_ARECORD_PROCEDURE_RETURN_TYPE_FIELD_INDEX = 4;
    public static final int PROCEDURE_ARECORD_PROCEDURE_DEFINITION_FIELD_INDEX = 5;
    public static final int PROCEDURE_ARECORD_PROCEDURE_LANGUAGE_FIELD_INDEX = 6;
    public static final ARecordType PROCEDURE_RECORDTYPE = MetadataRecordTypes.createRecordType(
            // RecordTypeName
            BADConstants.RECORD_TYPENAME_PROCEDURE,
            // FieldNames
            new String[] { BADConstants.DataverseName, BADConstants.ProcedureName, BADConstants.FIELD_NAME_ARITY,
                    BADConstants.FIELD_NAME_PARAMS, BADConstants.FIELD_NAME_RETURN_TYPE,
                    BADConstants.FIELD_NAME_DEFINITION, BADConstants.FIELD_NAME_LANGUAGE },
            // FieldTypes
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                    new AOrderedListType(BuiltinType.ASTRING, null), BuiltinType.ASTRING, BuiltinType.ASTRING,
                    BuiltinType.ASTRING },
            //IsOpen?
            true);

}
