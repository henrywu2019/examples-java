package io.github.streamingwithflink.uber;

import io.github.streamingwithflink.util.SafeCounterWithoutLock;
import org.apache.derby.iapi.types.DataType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.TableSinkBase;
import org.apache.flink.types.Row;

import java.util.Optional;

// refer to: https://github.com/apache/flink/blob/master/flink-table/flink-sql-client/src/main/java/org/apache/flink/table/client/gateway/local/CollectStreamTableSink.java

/**
 * 自定义RetractStreamTableSink
 * <p>
 * Table在内部被转换成具有Add(增加)和Retract(撤消/删除)的消息流，最终交由DataStream的SinkFunction处理。
 * DataStream里的数据格式是Tuple2类型,如Tuple2<Boolean, Row>。
 * Boolean是Add(增加)或Retract(删除)的flag(标识)。Row是真正的数据类型。
 * Table中的Insert被编码成一条Add消息。如Tuple2<True, Row>。
 * Table中的Update被编码成两条消息。一条删除消息Tuple2<False, Row>，一条增加消息Tuple2<True, Row>。
 */
class MazuRunningPodsSink implements RetractStreamTableSink<Row> {

    private TableSchema tableSchema;

    public static SafeCounterWithoutLock scwl = new SafeCounterWithoutLock();

    public MazuRunningPodsSink(String[] fieldNames, TypeInformation[] fieldTypes) {
        this.tableSchema = new TableSchema(fieldNames, fieldTypes);
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        final MazuRunningPodsSink copy = new MazuRunningPodsSink(fieldNames, fieldTypes);
        return copy;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return getTableSchema().toRowType();
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        //dataStream.print(); // for debugging
        dataStream.addSink(new MySinkFunction());
    }

    private static class MySinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> {
        public MySinkFunction() {
        }

        @Override
        public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
            Boolean flag = value.f0;
            if (flag) {
                System.out.println(scwl.getValue() + ". Add " + value);
            } else {
                System.out.println(scwl.getValue() + ". Del " + value);
            }
            scwl.increment();
        }
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        final TupleTypeInfo<Tuple2<Boolean, Row>> res = new TupleTypeInfo<>(Types.BOOLEAN(), getRecordType());
        return res;
    }

    @Override
    public String[] getFieldNames() {
        return tableSchema.getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return tableSchema.getFieldTypes();
    }
}
