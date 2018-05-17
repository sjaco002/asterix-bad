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
package org.apache.asterix.bad.test;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class BADAsterixHyracksIntegrationUtil extends AsterixHyracksIntegrationUtil {

    public static void main(String[] args) throws Exception {
        BADAsterixHyracksIntegrationUtil integrationUtil = new BADAsterixHyracksIntegrationUtil();
        try {
            integrationUtil.run(Boolean.getBoolean("cleanup.start"), Boolean.getBoolean("cleanup.shutdown"),
                    System.getProperty("external.lib", ""),
                    "asterixdb/asterix-opt/asterix-bad/src/main/resources/cc.conf");
        } catch (Exception e) {
            System.exit(1);
        }
    }

    protected void run(boolean cleanupOnStart, boolean cleanupOnShutdown, String ccConfPath, boolean wait)
            throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    deinit(cleanupOnShutdown);
                } catch (Exception e) {

                }
            }
        });
        init(cleanupOnStart, ccConfPath);
        while (wait) {
            Thread.sleep(10000);
        }
    }

    @Override
    protected void run(boolean cleanupOnStart, boolean cleanupOnShutdown, String loadExternalLibs, String ccConfPath)
            throws Exception {
        run(cleanupOnStart, cleanupOnShutdown, ccConfPath, true);
    }

}
