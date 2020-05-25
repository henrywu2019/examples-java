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

    public final static String[] SOURCE_FIELDS_NAMES = new String[]{
            "pod_name",
            "job_name",
            "multiplier",
            "start_time",
            "end_time"
    };

    public final static TypeInformation[] SOURCE_TYPE_INFO = new TypeInformation[]{
            Types.STRING(),
            Types.STRING(),
            Types.DOUBLE(),
            Types.LONG(),
            Types.LONG()
    };

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
        // The source is a datastream of String from Kinesis
        final DataStream<String> src = Utils.createSourceFromStaticConfig(env);
        final DataStream<Row> dataStream = src
                .map(value -> Utils.StringToPodMessageRaw(value)) // String to PodMessageRaw
                //.filter(new Utils.RunningPodFilter()) // Filter out end_time != -1
                .map(v -> Utils.ObjToRow(v))
                .returns(getReturnType()); // Convert PodMessageRaw to Row
        return dataStream;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return new RowTypeInfo(SOURCE_TYPE_INFO, SOURCE_FIELDS_NAMES);
    }

    @Override
    public TableSchema getTableSchema() {
        return new TableSchema(SOURCE_FIELDS_NAMES, SOURCE_TYPE_INFO);
    }
}