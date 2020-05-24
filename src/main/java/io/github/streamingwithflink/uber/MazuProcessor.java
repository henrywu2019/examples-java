package io.github.streamingwithflink.uber;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.twitter.chill.protobuf.ProtobufSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import podmsgenriched.MazuRecordEnriched;
import podmsgraw.MazuRecordRaw;

import java.util.Properties;

// https://code.uberinternal.com/diffusion/ATATGXS/browse/master/handler/batch_kubernetes_collector.go$406
public class MazuProcessor {
    // https://stackoverflow.com/questions/32453030/using-an-collectionsunmodifiablecollection-with-apache-flink
    public static void main(String[] args) throws Exception {
        RunStreamingAggregator();
    }

    private static final String region = "us-east-1";
    private static final String inputStreamName = "mazu-kubernetes-podwatcher-raw";
    private static final String outputStreamName = "mazu-kubernetes-podwatcher-enriched";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON");
        /*inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "1490195645");*/
        inputProperties.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "PROFILE");
        inputProperties.setProperty(ConsumerConfigConstants.AWS_PROFILE_NAME, "UberATGProd/ATGEng");
        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
    }

    private static FlinkKinesisProducer<String> createSinkFromStaticConfig() {
        System.out.println("sink");
        Properties outputProperties = new Properties();
        outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        FlinkKinesisProducer<String> sink = new FlinkKinesisProducer<>(new SimpleStringSchema(), outputProperties);
        sink.setDefaultStream(outputStreamName);
        sink.setDefaultPartition("0");
        return sink;
    }

    // 1. filter out to get running pods stream
    private static void RunStreamingAggregator() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<String> input = createSourceFromStaticConfig(env);
        final SingleOutputStreamOperator<MazuRecordRaw.PodMessageRaw> d= input
                .map(value -> StringToPodMessageRaw(value))
                .filter(new RunningPodFilter());
        d.print("henry");
        env.execute("kubernetes-job-podcost-aggreagator");
    }

    static final ReduceFunction<MazuRecordRaw.PodMessageRaw> mazuKubeReducer = new ReduceFunction<MazuRecordRaw.PodMessageRaw>(){ //Window Functions - one of ReduceFunction, AggregateFunction, FoldFunction or ProcessWindowFunction
        @Override
        public MazuRecordRaw.PodMessageRaw reduce(MazuRecordRaw.PodMessageRaw p1, MazuRecordRaw.PodMessageRaw p2) throws Exception {
            MazuRecordRaw.PodMessageRaw.Builder pmb = MazuRecordRaw.PodMessageRaw.newBuilder(p1);
            return pmb.setCpuRequested(1).setJobName("henry.wu").build();
        }};

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

    private static MazuRecordRaw.PodMessageRaw StringToPodMessageRaw(String d1) throws Exception {
        try {
            final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
            MazuRecordRaw.PodMessageRaw.Builder pmb = MazuRecordRaw.PodMessageRaw.newBuilder();
            JsonFormat.parser().merge(d1, pmb);
            return pmb.build();
        } catch(com.google.protobuf.InvalidProtocolBufferException e) {
            return  MazuRecordRaw.PodMessageRaw.newBuilder().build();
        }
    }

    private static class JobNameKeySelector
            implements KeySelector<MazuRecordRaw.PodMessageRaw, String> {
        @Override
        public String getKey(MazuRecordRaw.PodMessageRaw value) {
            return value.getJobName();
        }
    }

    private static class RunningPodFilter implements FilterFunction<MazuRecordRaw.PodMessageRaw> {
        @Override
        public boolean filter(MazuRecordRaw.PodMessageRaw value) throws Exception{
            return value.getEndTime()==-1;
        }
    }
}