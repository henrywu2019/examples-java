package io.github.streamingwithflink.uber;

import com.google.protobuf.util.JsonFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import podmsgraw.MazuRecordRaw;
import org.apache.flink.table.sources.StreamTableSource;

import java.util.Properties;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

// https://code.uberinternal.com/diffusion/ATATGXS/browse/master/handler/batch_kubernetes_collector.go$406
public class MazuProcessor {
    // https://stackoverflow.com/questions/32453030/using-an-collectionsunmodifiablecollection-with-apache-flink
    public static void main(String[] args) throws Exception {
        RunStreamingAggregator();
    }

    // 1. filter out to get running pods stream
    private static void RunStreamingAggregator() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final MazuStreamTableSource src = new MazuStreamTableSource();
        tableEnv.registerTableSource("running_pods_source", src);

        // define the field names and types
        final String[] fieldNames = {"job_name", "multiplier"};
        final TypeInformation[] fieldTypes = {Types.STRING(), Types.DOUBLE()};

        final MazuRunningPodsSink rsts = new MazuRunningPodsSink(fieldNames, fieldTypes);
        //final TableSink rsts = new CsvTableSink("/media/henry.wu/sandbox/data/running_pods_sink.csv", "|", 1, OVERWRITE);
        tableEnv.registerTableSink("henry_sink", fieldNames, fieldTypes, rsts);

        Table in = tableEnv.scan("running_pods_source");
        Table result = in.groupBy("job_name").select("job_name, multiplier.sum as total_multiplier");
        result.insertInto("henry_sink");

        env.execute("kubernetes-job-podcost-aggreagator");
    }

    // TODO: the transform calculation function should be added to this function!
    // 1. calculation 2. convert back to String
    private static String TransformPod(String d1) throws Exception {
        try {
            MazuRecordRaw.PodMessageRaw.Builder pmb = MazuRecordRaw.PodMessageRaw.newBuilder();
            JsonFormat.parser().merge(d1, pmb);
            MazuRecordRaw.PodMessageRaw m = pmb.setAsset("ass").setGpuCost(1.1).build(); // m is PodMessageRaw
            // calculation here
            return m.toString();
        }
        catch(com.google.protobuf.InvalidProtocolBufferException e) {
            System.out.println(d1);
            return "";
        }
    }





}