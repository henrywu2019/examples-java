package io.github.streamingwithflink.uber;

import com.google.protobuf.util.JsonFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.types.Row;
import podmsgraw.MazuRecordRaw;

import java.util.Properties;

public class Utils {
    public static final String region = "us-east-1";
    public static final String inputStreamName = "mazu-kubernetes-podwatcher-raw";
    public static final String outputStreamName = "mazu-kubernetes-podwatcher-enriched";

    public static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON");
        /*inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "1490195645");*/
        inputProperties.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "PROFILE");
        inputProperties.setProperty(ConsumerConfigConstants.AWS_PROFILE_NAME, "UberATGProd/ATGEng");
        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
    }

    public static FlinkKinesisProducer<String> createSinkFromStaticConfig() {
        Properties outputProperties = new Properties();
        outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, Utils.region);
        FlinkKinesisProducer<String> sink = new FlinkKinesisProducer<>(new SimpleStringSchema(), outputProperties);
        sink.setDefaultStream(Utils.outputStreamName);
        sink.setDefaultPartition("0");
        return sink;
    }

    public static MazuRecordRaw.PodMessageRaw StringToPodMessageRaw(String d1) throws Exception {
        try {
            final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
            MazuRecordRaw.PodMessageRaw.Builder pmb = MazuRecordRaw.PodMessageRaw.newBuilder();
            JsonFormat.parser().merge(d1, pmb);
            return pmb.build();
        } catch(com.google.protobuf.InvalidProtocolBufferException e) {
            return  MazuRecordRaw.PodMessageRaw.newBuilder().build();
        }
    }


    public static Row ObjToRow(MazuRecordRaw.PodMessageRaw data) {
        return Row.of(data.getJobName(), data.getMultiplier());
    }

    public static class RunningPodFilter implements FilterFunction<MazuRecordRaw.PodMessageRaw> {
        @Override
        public boolean filter(MazuRecordRaw.PodMessageRaw value) throws Exception{
            return value.getEndTime()==-1;
        }
    }

    public static class JobNameKeySelector
            implements KeySelector<MazuRecordRaw.PodMessageRaw, String> {
        @Override
        public String getKey(MazuRecordRaw.PodMessageRaw value) {
            return value.getJobName();
        }
    }
}
