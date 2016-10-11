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

import org.apache.asterix.app.cc.CompilerExtensionManager;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.translator.SessionConfig;

public class BADStatementExecutor extends QueryTranslator {

    public BADStatementExecutor(List<Statement> aqlStatements, SessionConfig conf,
            ILangCompilationProvider compliationProvider, CompilerExtensionManager ccExtensionManager) {
        super(aqlStatements, conf, compliationProvider, ccExtensionManager);
    }

    /*
    @Override
    protected void handleDataverseDropStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        super(metadataProvider, stmt, hcc);
        //TODO: need to drop channels and brokers
        //TODO: need to check if datasets or functions are in use by channels
    }*/

}
