package io.github.streamingwithflink.uber;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

// https://www.programcreek.com/java-api-examples/?code=dataArtisans%2Fflink-training-exercises%2Fflink-training-exercises-master%2Fsrc%2Fmain%2Fjava%2Fcom%2Fdataartisans%2Fflinktraining%2Fexercises%2Ftable_java%2Fsources%2FTaxiRideTableSource.java
class MazuStreamTableSource implements StreamTableSource<Row> {
    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env){
        // The source is a datastream of String from Kinesis
        DataStream<String> src =Utils.createSourceFromStaticConfig(env);
        final DataStream<Row> d= src
                .map(value -> Utils.StringToPodMessageRaw(value)) // String to PodMessageRaw
                .filter(new Utils.RunningPodFilter()) // Filter out end_time != -1
                .map(v -> Utils.ObjToRow(v))
                .returns(getReturnType()); // Convert PodMessageRaw to Row
        return d;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        final RowTypeInfo ri = new RowTypeInfo(new TypeInformation[]{Types.STRING(), Types.DOUBLE()},
                new String[] {"job_name", "multiplier"});
        return ri;
    }

    @Override
    public TableSchema getTableSchema() {
        return new TableSchema(
                new String[] {"job_name", "multiplier"},
                new TypeInformation[] {Types.STRING(), Types.DOUBLE()});
    }
}