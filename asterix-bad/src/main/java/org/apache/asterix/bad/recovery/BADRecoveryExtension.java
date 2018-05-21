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
package org.apache.asterix.bad.recovery;

import java.util.List;

import org.apache.asterix.app.cc.IGlobalRecoveryExtension;
import org.apache.asterix.common.api.ExtensionId;
import org.apache.asterix.common.cluster.IGlobalRecoveryManager;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;

public class BADRecoveryExtension implements IGlobalRecoveryExtension {

    public static final ExtensionId BAD_RECOVERY_EXTENSION_ID =
            new ExtensionId(BADRecoveryExtension.class.getSimpleName(), 0);

    @Override
    public ExtensionId getId() {
        return BAD_RECOVERY_EXTENSION_ID;
    }

    @Override
    public void configure(List<Pair<String, String>> args) {
    }

    @Override
    public IGlobalRecoveryManager getGlobalRecoveryManager(ICCServiceContext serviceCtx, IHyracksClientConnection hcc,
            IStorageComponentProvider componentProvider) {
        return new BADGlobalRecoveryManager(serviceCtx, hcc, componentProvider);
    }
}
