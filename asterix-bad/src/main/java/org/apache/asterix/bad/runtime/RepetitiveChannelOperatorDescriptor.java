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
package org.apache.asterix.bad.runtime;

import java.util.logging.Logger;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.bad.BADConstants;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

/**
 * A repetitive channel operator, which uses a Java timer to run a given query periodically
 */
public class RepetitiveChannelOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(RepetitiveChannelOperatorDescriptor.class.getName());

    /** The unique identifier of the job. **/
    protected final EntityId entityId;

    protected final JobSpecification jobSpec;

    private final String duration;

    private String strIP;
    private int port;

    public RepetitiveChannelOperatorDescriptor(JobSpecification spec, String dataverseName, String channelName,
            String duration, JobSpecification channeljobSpec, String strIP, int port) {
        super(spec, 0, 0);
        this.entityId = new EntityId(BADConstants.CHANNEL_EXTENSION_NAME, dataverseName, channelName);
        this.jobSpec = channeljobSpec;
        this.duration = duration;
        this.strIP = strIP;
        this.port = port;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        ActiveRuntimeId runtimeId = new ActiveRuntimeId(entityId,
                RepetitiveChannelOperatorNodePushable.class.getSimpleName(), partition);
        return new RepetitiveChannelOperatorNodePushable(ctx, runtimeId, jobSpec, duration, strIP, port);
    }

    public String getDuration() {
        return duration;
    }

    public EntityId getEntityId() {
        return entityId;
    }

    public JobSpecification getJobSpec() {
        return jobSpec;
    }

}
