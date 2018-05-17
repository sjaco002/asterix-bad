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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class BADRecoveryTest {

    private static final String PATH_ACTUAL = "target" + File.separator + "rttest" + File.separator;
    private static final String PATH_BASE = "src/test/resources/recoveryts/";
    private TestCaseContext tcCtx;
    private static ProcessBuilder pb;
    private static Map<String, String> env;
    private final TestExecutor testExecutor = new TestExecutor();

    @BeforeClass
    public static void setUp() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
        pb = new ProcessBuilder();
        env = pb.environment();
        env.put("JAVA_HOME", System.getProperty("java.home"));

    }

    @AfterClass
    public static void tearDown() throws Exception {
    }

    @Parameters(name = "RecoveryIT {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE))) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }

    public BADRecoveryTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }


    @Test
    public void test() throws Exception {
        BADAsterixHyracksIntegrationUtil integrationUtil = new BADAsterixHyracksIntegrationUtil();
        try {
            integrationUtil.run(false, false, "src/main/resources/cc.conf", false);
        } catch (Exception e) {
            System.exit(1);
        }
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, pb, false);
        integrationUtil.deinit(false);
    }

}
