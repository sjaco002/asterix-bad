/*
 * Copyright 2009-2015 by The Regents of the University of California
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IExtensionMetadataEntity;

/**
 * Metadata describing a channel.
 */
public class Channel implements IExtensionMetadataEntity {

    private static final long serialVersionUID = 1L;

    /** A unique identifier for the channel */
    protected final EntityId channelId;
    private final String subscriptionsDatasetName;
    private final String resultsDatasetName;
    private final String duration;
    private final String channelBody;
    private final FunctionSignature function;
    private final List<String> functionAsPath;
    /*
    Dependencies are stored as an array of size two:
    element 0 is a list of dataset dependencies
    -stored as lists of [DataverseName, Dataset] for the datasets
    element 1 is a list of function dependencies
    -stored as lists of [DataverseName, FunctionName, Arity] for the functions
    */
    private final List<List<List<String>>> dependencies;

    public Channel(String dataverseName, String channelName, String subscriptionsDataset, String resultsDataset,
            FunctionSignature function, String duration, List<List<List<String>>> dependencies, String channelBody) {
        this.channelId = new EntityId(BADConstants.CHANNEL_EXTENSION_NAME, dataverseName, channelName);
        this.function = function;
        this.duration = duration;
        this.resultsDatasetName = resultsDataset;
        this.subscriptionsDatasetName = subscriptionsDataset;
        this.channelBody = channelBody;
        if (this.function.getNamespace() == null) {
            this.function.setNamespace(dataverseName);
        }
        functionAsPath = Arrays.asList(this.function.getNamespace(), this.function.getName(),
                Integer.toString(this.function.getArity()));
        if (dependencies == null) {
            this.dependencies = new ArrayList<>();
            this.dependencies.add(new ArrayList<>());
            this.dependencies.add(new ArrayList<>());
            List<String> resultsList = Arrays.asList(dataverseName, resultsDatasetName);
            List<String> subscriptionList = Arrays.asList(dataverseName, subscriptionsDatasetName);
            this.dependencies.get(0).add(resultsList);
            this.dependencies.get(0).add(subscriptionList);
            this.dependencies.get(1).add(functionAsPath);
        } else {
            this.dependencies = dependencies;
        }
    }

    public EntityId getChannelId() {
        return channelId;
    }

    public List<List<List<String>>> getDependencies() {
        return dependencies;
    }

    public String getSubscriptionsDataset() {
        return subscriptionsDatasetName;
    }

    public String getResultsDatasetName() {
        return resultsDatasetName;
    }

    public String getDuration() {
        return duration;
    }

    public String getChannelBody() {
        return channelBody;
    }

    public List<String> getFunctionAsPath() {
        return functionAsPath;
    }

    public FunctionSignature getFunction() {
        return function;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Channel)) {
            return false;
        }
        Channel otherDataset = (Channel) other;
        if (!otherDataset.channelId.equals(channelId)) {
            return false;
        }
        return true;
    }

    @Override
    public ExtensionMetadataDatasetId getDatasetId() {
        return BADMetadataIndexes.BAD_CHANNEL_INDEX_ID;
    }
}