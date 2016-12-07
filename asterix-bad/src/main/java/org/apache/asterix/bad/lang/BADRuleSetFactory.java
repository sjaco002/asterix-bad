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
package org.apache.asterix.bad.lang;

import java.util.List;

import org.apache.asterix.bad.rules.InsertBrokerNotifierForChannelRule;
import org.apache.asterix.compiler.provider.DefaultRuleSetFactory;
import org.apache.asterix.compiler.provider.IRuleSetFactory;
import org.apache.asterix.optimizer.base.RuleCollections;
import org.apache.asterix.optimizer.rules.UnnestToDataScanRule;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialOnceRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class BADRuleSetFactory implements IRuleSetFactory {

    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getLogicalRewrites()
            throws AlgebricksException {
        List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRuleSet = DefaultRuleSetFactory.buildLogical();
        if (logicalRuleSet.size() != 14) {
            throw new AlgebricksException("Incorrect RuleSet");
        }
        List<IAlgebraicRewriteRule> normalizationCollection = RuleCollections.buildNormalizationRuleCollection();

        for (int i = 0; i < normalizationCollection.size(); i++) {
            IAlgebraicRewriteRule rule = normalizationCollection.get(i);
            if (rule instanceof UnnestToDataScanRule) {
                normalizationCollection.add(i + 1, new InsertBrokerNotifierForChannelRule());
                break;
            }
        }
        SequentialOnceRuleController seqOnceCtrl = new SequentialOnceRuleController(true);
        logicalRuleSet.set(3, new Pair<>(seqOnceCtrl, normalizationCollection));
        logicalRuleSet.set(7, new Pair<>(seqOnceCtrl, normalizationCollection));
        return logicalRuleSet;
    }

    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getPhysicalRewrites() {
        return DefaultRuleSetFactory.buildPhysical();
    }

}
