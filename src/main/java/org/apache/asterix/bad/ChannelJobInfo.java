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
package org.apache.asterix.bad;

import java.util.List;

import org.apache.asterix.active.ActiveJob;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.bad.BADConstants.ChannelJobType;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class ChannelJobInfo extends ActiveJob {

    private static final long serialVersionUID = 1L;
    private List<String> locations;

    public ChannelJobInfo(EntityId entityId, JobId jobId, ActivityState state, JobSpecification spec) {
        super(entityId, jobId, state, ChannelJobType.REPETITIVE, spec);
    }

    public List<String> getLocations() {
        return locations;

    }

    public void setLocations(List<String> locations) {
        this.locations = locations;
    }

}
