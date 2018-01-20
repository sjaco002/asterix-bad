/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.asterix.bad.metadata;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.entitytupletranslators.AbstractTupleTranslator;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Channel metadata entity to an ITupleReference and vice versa.
 */
public class ChannelTupleTranslator extends AbstractTupleTranslator<Channel> {
    // Field indexes of serialized Feed in a tuple.
    // Key field.
    public static final int CHANNEL_DATAVERSE_NAME_FIELD_INDEX = 0;

    public static final int CHANNEL_NAME_FIELD_INDEX = 1;

    // Payload field containing serialized feed.
    public static final int CHANNEL_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    private ISerializerDeserializer<ARecord> recordSerDes = SerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BADMetadataRecordTypes.CHANNEL_RECORDTYPE);

    private transient OrderedListBuilder dependenciesListBuilder = new OrderedListBuilder();
    private transient OrderedListBuilder dependencyListBuilder = new OrderedListBuilder();
    private transient OrderedListBuilder dependencyNameListBuilder = new OrderedListBuilder();
    private transient AOrderedListType stringList = new AOrderedListType(BuiltinType.ASTRING, null);
    private transient AOrderedListType ListofLists =
            new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null);

    public ChannelTupleTranslator(boolean getTuple) {
        super(getTuple, BADMetadataIndexes.NUM_FIELDS_CHANNEL_IDX);
    }

    @Override
    public Channel getMetadataEntityFromTuple(ITupleReference frameTuple) throws HyracksDataException {
        byte[] serRecord = frameTuple.getFieldData(CHANNEL_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(CHANNEL_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(CHANNEL_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord channelRecord = recordSerDes.deserialize(in);
        return createChannelFromARecord(channelRecord);
    }

    private Channel createChannelFromARecord(ARecord channelRecord) {
        Channel channel = null;
        String dataverseName = ((AString) channelRecord
                .getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_DATAVERSE_NAME_FIELD_INDEX)).getStringValue();
        String channelName =
                ((AString) channelRecord.getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_CHANNEL_NAME_FIELD_INDEX))
                        .getStringValue();
        String subscriptionsName = ((AString) channelRecord
                .getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_SUBSCRIPTIONS_NAME_FIELD_INDEX)).getStringValue();
        String resultsName =
                ((AString) channelRecord.getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_RESULTS_NAME_FIELD_INDEX))
                        .getStringValue();

        IACursor cursor = ((AOrderedList) channelRecord
                .getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_FUNCTION_FIELD_INDEX)).getCursor();
        List<String> functionSignature = new ArrayList<>();
        while (cursor.next()) {
            functionSignature.add(((AString) cursor.get()).getStringValue());
        }

        String duration =
                ((AString) channelRecord.getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_DURATION_FIELD_INDEX))
                        .getStringValue();

        IACursor dependenciesCursor = ((AOrderedList) channelRecord
                .getValueByPos(BADMetadataRecordTypes.CHANNEL_ARECORD_DEPENDENCIES_FIELD_INDEX)).getCursor();
        List<List<List<String>>> dependencies = new ArrayList<>();
        AOrderedList dependencyList;
        AOrderedList qualifiedList;
        int i = 0;
        while (dependenciesCursor.next()) {
            dependencies.add(new ArrayList<>());
            dependencyList = (AOrderedList) dependenciesCursor.get();
            IACursor qualifiedDependencyCursor = dependencyList.getCursor();
            int j = 0;
            while (qualifiedDependencyCursor.next()) {
                qualifiedList = (AOrderedList) qualifiedDependencyCursor.get();
                IACursor qualifiedNameCursor = qualifiedList.getCursor();
                dependencies.get(i).add(new ArrayList<>());
                while (qualifiedNameCursor.next()) {
                    dependencies.get(i).get(j).add(((AString) qualifiedNameCursor.get()).getStringValue());
                }
                j++;
            }
            i++;

        }

        FunctionSignature signature = new FunctionSignature(functionSignature.get(0), functionSignature.get(1),
                Integer.parseInt(functionSignature.get(2)));

        channel = new Channel(dataverseName, channelName, subscriptionsName, resultsName, signature, duration,
                dependencies);
        return channel;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Channel channel) throws HyracksDataException, MetadataException {
        // write the key in the first fields of the tuple

        tupleBuilder.reset();
        aString.setValue(channel.getChannelId().getDataverse());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(channel.getChannelId().getEntityName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(BADMetadataRecordTypes.CHANNEL_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(channel.getChannelId().getDataverse());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(channel.getChannelId().getEntityName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_CHANNEL_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(channel.getSubscriptionsDataset());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_SUBSCRIPTIONS_NAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(channel.getResultsDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_RESULTS_NAME_FIELD_INDEX, fieldValue);

        // write field 4
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset(stringList);
        for (String pathPart : channel.getFunctionAsPath()) {
            itemValue.reset();
            aString.setValue(pathPart);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_FUNCTION_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        aString.setValue(channel.getDuration());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_DURATION_FIELD_INDEX, fieldValue);

        // write field 6
        dependenciesListBuilder.reset((AOrderedListType) BADMetadataRecordTypes.CHANNEL_RECORDTYPE
                .getFieldTypes()[BADMetadataRecordTypes.CHANNEL_ARECORD_DEPENDENCIES_FIELD_INDEX]);
        List<List<List<String>>> dependenciesList = channel.getDependencies();
        for (List<List<String>> dependencies : dependenciesList) {
            dependencyListBuilder.reset(ListofLists);
            for (List<String> dependency : dependencies) {
                dependencyNameListBuilder.reset(stringList);
                for (String subName : dependency) {
                    itemValue.reset();
                    aString.setValue(subName);
                    stringSerde.serialize(aString, itemValue.getDataOutput());
                    dependencyNameListBuilder.addItem(itemValue);
                }
                itemValue.reset();
                dependencyNameListBuilder.write(itemValue.getDataOutput(), true);
                dependencyListBuilder.addItem(itemValue);

            }
            itemValue.reset();
            dependencyListBuilder.write(itemValue.getDataOutput(), true);
            dependenciesListBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        dependenciesListBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(BADMetadataRecordTypes.CHANNEL_ARECORD_DEPENDENCIES_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);

        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}