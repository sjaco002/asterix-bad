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
package org.apache.asterix.bad.lang.statement;

import java.io.DataOutput;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.algebra.extension.IExtensionStatement;
import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.bad.BADConstants;
import org.apache.asterix.bad.ChannelJobService;
import org.apache.asterix.bad.lang.BADLangExtension;
import org.apache.asterix.bad.metadata.PrecompiledJobEventListener;
import org.apache.asterix.bad.metadata.PrecompiledJobEventListener.PrecompiledType;
import org.apache.asterix.bad.metadata.Procedure;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.translator.ConstantHelper;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.PreDistributedId;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class ExecuteProcedureStatement implements IExtensionStatement {

    private final String dataverseName;
    private final String procedureName;
    private final int arity;
    private final List<Expression> argList;

    public ExecuteProcedureStatement(String dataverseName, String procedureName, int arity, List<Expression> argList) {
        this.dataverseName = dataverseName;
        this.procedureName = procedureName;
        this.arity = arity;
        this.argList = argList;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getProcedureName() {
        return procedureName;
    }

    public int getArity() {
        return arity;
    }

    @Override
    public byte getKind() {
        return Kind.EXTENSION;
    }

    @Override
    public byte getCategory() {
        return Category.UPDATE;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

    @Override
    public void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId)
            throws HyracksDataException, AlgebricksException {
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        String dataverse = ((QueryTranslator) statementExecutor).getActiveDataverse(new Identifier(dataverseName));
        boolean txnActive = false;
        EntityId entityId = new EntityId(BADConstants.PROCEDURE_KEYWORD, dataverse, procedureName);
        PrecompiledJobEventListener listener = (PrecompiledJobEventListener) activeEventHandler.getListener(entityId);
        Procedure procedure = null;

        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            txnActive = true;
            procedure = BADLangExtension.getProcedure(mdTxnCtx, dataverse, procedureName, Integer.toString(getArity()));
            if (procedure == null) {
                throw new AlgebricksException("There is no procedure with this name " + procedureName + ".");
            }
            Map<byte[], byte[]> contextRuntimeVarMap = createContextRuntimeMap(procedure);
            PreDistributedId preDistributedId = listener.getPredistributedId();
            if (procedure.getDuration().equals("")) {
                JobId jobId = hcc.startJob(preDistributedId, contextRuntimeVarMap);

                if (listener.getType() == PrecompiledType.QUERY) {
                    hcc.waitForCompletion(jobId);
                    //ResultReader resultReader = new ResultReader(hdc, jobId, metadataProvider.getResultSetId());
                    ResultReader resultReader =
                            new ResultReader(listener.getResultDataset(), jobId, listener.getResultId());

                    ResultUtil.printResults(appCtx, resultReader,
                            ((QueryTranslator) statementExecutor).getSessionOutput(), new Stats(), null);
                }

            } else {
                ScheduledExecutorService ses =
                        ChannelJobService.startJob(null, EnumSet.noneOf(JobFlag.class), preDistributedId, hcc,
                                ChannelJobService.findPeriod(procedure.getDuration()), contextRuntimeVarMap, entityId);
                listener.storeDistributedInfo(preDistributedId, ses, listener.getResultDataset(),
                        listener.getResultId());
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            txnActive = false;
        } catch (Exception e) {
            e.printStackTrace();
            if (txnActive) {
                QueryTranslator.abort(e, e, mdTxnCtx);
            }
            throw new HyracksDataException(e);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    private Map<byte[], byte[]> createContextRuntimeMap(Procedure procedure)
            throws AsterixException, HyracksDataException {
        Map<byte[], byte[]> map = new HashMap<>();
        if (procedure.getParams().size() != argList.size()) {
            throw AsterixException.create(ErrorCode.COMPILATION_INVALID_PARAMETER_NUMBER,
                    procedure.getEntityId().getEntityName(), argList.size());
        }
        ArrayBackedValueStorage abvsKey = new ArrayBackedValueStorage();
        DataOutput dosKey = abvsKey.getDataOutput();
        ArrayBackedValueStorage abvsValue = new ArrayBackedValueStorage();
        DataOutput dosValue = abvsValue.getDataOutput();

        for (int i = 0; i < procedure.getParams().size(); i++) {
            if (!(argList.get(i) instanceof LiteralExpr)) {
                //TODO handle nonliteral arguments to procedure
                throw AsterixException.create(ErrorCode.TYPE_UNSUPPORTED, procedure.getEntityId().getEntityName(),
                        argList.get(i).getClass());
            }
            //Turn the argument name into a byte array
            IAObject str = new AString(procedure.getParams().get(i));
            abvsKey.reset();
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(str.getType()).serialize(str, dosKey);
            //We do not save the type tag of the string key
            byte[] key = new byte[abvsKey.getLength() - 1];
            System.arraycopy(abvsKey.getByteArray(), 1, key, 0, abvsKey.getLength() - 1);

            //Turn the argument value into a byte array
            IAObject object = ConstantHelper.objectFromLiteral(((LiteralExpr) argList.get(i)).getValue());
            abvsValue.reset();
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(object.getType()).serialize(object,
                    dosValue);
            byte[] value = new byte[abvsValue.getLength()];
            System.arraycopy(abvsValue.getByteArray(), abvsValue.getStartOffset(), value, 0, abvsValue.getLength());

            map.put(key, value);
        }
        return map;
    }

}