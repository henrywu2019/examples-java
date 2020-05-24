package io.github.streamingwithflink.externalsink;

import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.table.api.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.sources.*;
//import org.apache.flink.table.api.java.*;
import org.apache.flink.table.api.*;
//import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.*;
import org.apache.flink.types.Row;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

public class MyDynamicTable {
    public static void main(String[] args) throws Exception {
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final org.apache.flink.table.api.java.StreamTableEnvironment tableEnv = org.apache.flink.table.api.java.StreamTableEnvironment.create(env);

        CsvTableSource csvTableSource = new CsvTableSource(
                "/media/henry.wu/sandbox/data/flink_data_source.csv",
                new String[] { "name", "id", "score", "comments" },
                new TypeInformation<?>[] {
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING(),
                        Types.STRING()
                });
        TypeInformation<Row> tr = csvTableSource.getReturnType();
        DataStream<Row> ds = csvTableSource.getDataStream(env);
        TypeInformation<Row> ti = csvTableSource.getDataStream(env).getType();

        tableEnv.registerTableSource("henry_csv", csvTableSource);
        TableSink sink = new CsvTableSink("/media/henry.wu/sandbox/data/flink_data_sink.csv", "|", 1, OVERWRITE);
        sink = sink.configure(new String[] { "name" }, new TypeInformation<?>[] { Types.STRING() });
        tableEnv.registerTableSink("henry_sink", sink);

        Table in = tableEnv.scan("henry_csv");
        Table result = in.select("name");
        result.insertInto("henry_sink");
        //result.writeToSink(sink);
        try {
            env.execute();
        } catch (Exception e) {

        }
    }
}
