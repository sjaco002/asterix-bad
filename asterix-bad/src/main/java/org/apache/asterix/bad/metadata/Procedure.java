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

import java.util.List;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IExtensionMetadataEntity;

public class Procedure implements IExtensionMetadataEntity {
    private static final long serialVersionUID = 1L;
    public static final String LANGUAGE_AQL = "AQL";
    public static final String LANGUAGE_JAVA = "JAVA";

    public static final String RETURNTYPE_VOID = "VOID";
    public static final String NOT_APPLICABLE = "N/A";

    private final EntityId procedureId;
    private final int arity;
    private final List<String> params;
    private final String body;
    private final String returnType;
    private final String language;

    public Procedure(String dataverseName, String functionName, int arity, List<String> params, String returnType,
            String functionBody, String language) {
        this.procedureId = new EntityId(BADConstants.PROCEDURE_KEYWORD, dataverseName, functionName);
        this.params = params;
        this.body = functionBody;
        this.returnType = returnType == null ? RETURNTYPE_VOID : returnType;
        this.language = language;
        this.arity = arity;
    }

    public EntityId getEntityId() {
        return procedureId;
    }

    public List<String> getParams() {
        return params;
    }

    public String getBody() {
        return body;
    }

    public String getReturnType() {
        return returnType;
    }

    public String getLanguage() {
        return language;
    }

    public int getArity() {
        return arity;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Procedure)) {
            return false;
        }
        Procedure otherDataset = (Procedure) other;
        if (!otherDataset.procedureId.equals(procedureId)) {
            return false;
        }
        return true;
    }

    @Override
    public ExtensionMetadataDatasetId getDatasetId() {
        return BADMetadataIndexes.BAD_PROCEDURE_INDEX_ID;
    }
}
