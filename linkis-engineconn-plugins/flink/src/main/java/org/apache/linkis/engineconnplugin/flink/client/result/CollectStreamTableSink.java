/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.linkis.engineconnplugin.flink.client.result;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.experimental.CollectSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.net.InetAddress;

/** Table sink for collecting the results locally using sockets. */
public class CollectStreamTableSink implements RetractStreamTableSink<Row> {

    private final InetAddress targetAddress;
    private final int targetPort;
    private final TypeSerializer<Tuple2<Boolean, Row>> serializer;
    private final TableSchema tableSchema;

    public CollectStreamTableSink(
            InetAddress targetAddress,
            int targetPort,
            TypeSerializer<Tuple2<Boolean, Row>> serializer,
            TableSchema tableSchema) {
        this.targetAddress = targetAddress;
        this.targetPort = targetPort;
        this.serializer = serializer;
        this.tableSchema = TableSchemaUtils.checkOnlyPhysicalColumns(tableSchema);
    }

    @Override
    public CollectStreamTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return new CollectStreamTableSink(targetAddress, targetPort, serializer, tableSchema);
    }

    // Retract stream sinks work only with the old type system.
    @Override
    public String[] getFieldNames() {
        return tableSchema.getFieldNames();
    }

    // Retract stream sinks work only with the old type system.
    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return tableSchema.getFieldTypes();
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return getTableSchema().toRowType();
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> stream) {
        // add sink
        return stream
                .addSink(new CollectSink<>(targetAddress, targetPort, serializer))
                .name("SQL Client Stream Collect Sink")
                .setParallelism(1);
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo<>(Types.BOOLEAN, getRecordType());
    }
}