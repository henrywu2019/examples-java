package io.github.streamingwithflink.uber;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.types.Row;

// https://code.uberinternal.com/diffusion/ATATGXS/browse/master/handler/batch_kubernetes_collector.go$406
public class MazuProcessor {

    public final static String SOURCE_NAME = "running_pods_source";
    public final static String SINK_NAME = "running_pods_sink";

    // https://stackoverflow.com/questions/32453030/using-an-collectionsunmodifiablecollection-with-apache-flink
    public static void main(String[] args) throws Exception {
        RunStreamingAggregator();
    }

    public final static String[] SINK_FIELDS_NAMES = new String[]{
            "pod_name",
            "job_name",
            "multiplier",
            "start_time",
            "end_time"
    };

    public final static TypeInformation[] SINK_TYPE_INFO = new TypeInformation[]{
            Types.STRING(),
            Types.STRING(),
            Types.DOUBLE(),
            Types.LONG(),
            Types.LONG()
    };

    // 1. filter out to get running pods stream
    private static void RunStreamingAggregator() throws Exception {
        final StreamExecutionEnvironment exeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(exeEnv);

        // Create source and sink
        final MazuStreamTableSource src = new MazuStreamTableSource();
        final RetractStreamTableSink<Row> rsts = new MazuRunningPodsSink(MazuStreamTableSource.SOURCE_FIELDS_NAMES,
                MazuStreamTableSource.SOURCE_TYPE_INFO);

        // Register source and sink
        tableEnv.registerTableSource(SOURCE_NAME, src);
        //final TableSink rsts = new CsvTableSink("/media/henry.wu/sandbox/data/running_pods_sink.csv", "|", 1, OVERWRITE);
        tableEnv.registerTableSink(SINK_NAME,
                SINK_FIELDS_NAMES,
                SINK_TYPE_INFO,
                rsts);

        // https://stackoverflow.com/questions/3491329/group-by-with-maxdate
        // Query source table and insert to sink table
        // option 1
        /*final Table targetTable = tableEnv.scan(SOURCE_NAME);
        final Table result = targetTable
                .groupBy("job_name")
                .select("job_name, multiplier.sum as multiplier, max(end_time) as end_time");
                //.select("pod_name, job_name, multiplier, start_time");
        result.insertInto(SINK_NAME);*/

        // option 2
        String sql = "insert into " + SINK_NAME
                + " select t.pod_name, t.job_name, t.multiplier, t.start_time, r.last_time" +
                " from (select pod_name, max(end_time) as last_time from "
                + SOURCE_NAME
                + " group by pod_name) r inner join " + SOURCE_NAME + " t on t.pod_name=r.pod_name and t.end_time=r.last_time";
        tableEnv.sqlUpdate(sql);

        exeEnv.execute(MazuProcessor.class.getSimpleName());
    }
}