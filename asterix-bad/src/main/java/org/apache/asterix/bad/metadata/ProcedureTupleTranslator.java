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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.entitytupletranslators.AbstractTupleTranslator;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Procedure metadata entity to an ITupleReference and vice versa.
 */
public class ProcedureTupleTranslator extends AbstractTupleTranslator<Procedure> {
    // Field indexes of serialized Procedure in a tuple.
    // First key field.
    public static final int PROCEDURE_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Second key field.
    public static final int PROCEDURE_PROCEDURE_NAME_TUPLE_FIELD_INDEX = 1;
    // Third key field.
    public static final int PROCEDURE_ARITY_TUPLE_FIELD_INDEX = 2;

    // Payload field containing serialized Procedure.
    public static final int PROCEDURE_PAYLOAD_TUPLE_FIELD_INDEX = 3;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = SerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BADMetadataRecordTypes.PROCEDURE_RECORDTYPE);

    protected ProcedureTupleTranslator(boolean getTuple) {
        super(getTuple, BADMetadataIndexes.NUM_FIELDS_PROCEDURE_IDX);
    }

    @Override
    public Procedure getMetadataEntityFromTuple(ITupleReference frameTuple) throws IOException {
        byte[] serRecord = frameTuple.getFieldData(PROCEDURE_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(PROCEDURE_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(PROCEDURE_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord procedureRecord = recordSerDes.deserialize(in);
        return createProcedureFromARecord(procedureRecord);
    }

    private Procedure createProcedureFromARecord(ARecord procedureRecord) {
        String dataverseName =
 ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_DATAVERSENAME_FIELD_INDEX))
                        .getStringValue();
        String procedureName =
 ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_NAME_FIELD_INDEX))
                        .getStringValue();
        String arity = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_ARITY_FIELD_INDEX)).getStringValue();

        IACursor cursor = ((AOrderedList) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_PARAM_LIST_FIELD_INDEX)).getCursor();
        List<String> params = new ArrayList<String>();
        while (cursor.next()) {
            params.add(((AString) cursor.get()).getStringValue());
        }

        String returnType = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_RETURN_TYPE_FIELD_INDEX))
                        .getStringValue();

        String definition = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_DEFINITION_FIELD_INDEX))
                        .getStringValue();

        String language = ((AString) procedureRecord
                .getValueByPos(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_LANGUAGE_FIELD_INDEX))
                        .getStringValue();

        return new Procedure(dataverseName, procedureName, Integer.parseInt(arity), params, returnType, definition,
                language);

    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Procedure procedure) throws IOException, MetadataException {
        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(procedure.getEntityId().getDataverse());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(procedure.getEntityId().getEntityName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(procedure.getArity() + "");
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the fourth field of the tuple

        recordBuilder.reset(BADMetadataRecordTypes.PROCEDURE_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(procedure.getEntityId().getDataverse());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(procedure.getEntityId().getEntityName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(procedure.getArity() + "");
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_ARITY_FIELD_INDEX, fieldValue);

        // write field 3
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset((AOrderedListType) BADMetadataRecordTypes.PROCEDURE_RECORDTYPE
                .getFieldTypes()[BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_PARAM_LIST_FIELD_INDEX]);
        for (String param : procedure.getParams()) {
            itemValue.reset();
            aString.setValue(param);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_PARAM_LIST_FIELD_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(procedure.getReturnType());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_RETURN_TYPE_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        aString.setValue(procedure.getBody());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_DEFINITION_FIELD_INDEX, fieldValue);

        // write field 6
        fieldValue.reset();
        aString.setValue(procedure.getLanguage());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(BADMetadataRecordTypes.PROCEDURE_ARECORD_PROCEDURE_LANGUAGE_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

}
