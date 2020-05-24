package io.github.streamingwithflink.uber;

import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KinesisTableSource{}

/*
class KinesisTableSource implements StreamTableSourceFactory<Row> {
    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put("update-mode", "append");
        context.put("connector.type", "my-system");
        return context;
    }
    @Override
    public List<String> supportedProperties() {
        List<String> list = new ArrayList<>();
        list.add("connector.debug");
        return list;
    }
    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        boolean isDebug = Boolean.valueOf(properties.get("connector.debug"));
        // additional validation of the passed properties can also happen here
        return new MySystemAppendTableSource(isDebug);
    }
}*/

/*
// https://stackoverflow.com/questions/58628681/tablesource-returned-a-datastream-that-does-not-match-with-the-type-declared-by
public class KinesisTableSource implements StreamTableSource<Row> {
    private final InitConfig initialisationConfig;
    private final StreamTableSource updateSource;

    public SdpUpdatedTableSource(InitConfig initialisationConfig, StreamTableSource updateSource) {
        this.initialisationConfig = initialisationConfig;
        this.updateSource = updateSource;
    }

    @Override
    public DataStream<Row> getDataStream(final StreamExecutionEnvironment env) {
        String initializationEndpoint = initialisationConfig.get("endpoint");
        String[] fieldsName = updateSource.getTableSchema().getFieldNames();

        DataStream<Row> stream = updateSource.getDataStream(env)
                .map(RowToMapObject(fieldsName))                    // Parse the row into a Map<String, Object> object
                .map(MyRichMapFunction(initializationEndpoint))     // Rich Map Function that initialize some data in the open() function using initializationEndpoint
                // And fuse the stored data & update data in the map. Output is a Map<String,Object> with same structure as the one in input
                .map(m->(Row)m);                                    // Parse back the Map into a Row

        return stream;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return updateSource.getReturnType();
    }

    @Override
    public TableSchema getTableSchema() {
        return updateSource.getTableSchema();
    }

    @Override
    public String explainSource() {
        return initialisationConfig.getId()+"_"+updateSource.explainSource();
    }
}*/