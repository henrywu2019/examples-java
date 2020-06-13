package io.github.streamingwithflink.mazu;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.twitter.chill.protobuf.ProtobufSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
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
import podmsgenriched.MazuRecordEnriched.PodMessageEnriched;
import podmsgraw.MazuRecordRaw.PodMessageRaw;

import java.util.Properties;

//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import akka.remote.serialization.ProtobufSerializer;

// https://code.uberinternal.com/diffusion/ATATGXS/browse/master/handler/batch_kubernetes_collector.go$406
public class StreamAggregator {
    private static final String region = "us-east-1";
    private static final String inputStreamName = "mazu-kubernetes-podwatcher-raw";
    private static final String outputStreamName = "mazu-kubernetes-podwatcher-enriched";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON");
        //inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
        //inputProperties.setProperty(ConsumerConfigConstants.AWS_PROFILE_PATH, "/home/henry.wu/.aws/credentials");
        inputProperties.setProperty(ConsumerConfigConstants.AWS_PROFILE_NAME, "UberATGProd/ATGEng");
        /*inputProperties.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "ASIAQEFGGIR3FMW7BCVB");
        inputProperties.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "f6eOCovn0Knb8QjyTO6gkoe9QIpZxii7TJ9EJo6X");*/
        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
    }

    private static FlinkKinesisProducer<String> createSinkFromStaticConfig() {
        System.out.println("sink");
        Properties outputProperties = new Properties();
        outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        /*outputProperties.setProperty(ConsumerConfigConstants.AWS_PROFILE_PATH, "/home/henry.wu/.aws/credentials");
        outputProperties.setProperty(ConsumerConfigConstants.AWS_PROFILE_NAME, "UberATGProd/ATGEng");
        outputProperties.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "ASIAQEFGGIR3FMW7BCVB");
        outputProperties.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "f6eOCovn0Knb8QjyTO6gkoe9QIpZxii7TJ9EJo6X");*/
        FlinkKinesisProducer<String> sink = new FlinkKinesisProducer<>(new SimpleStringSchema(), outputProperties);
        sink.setDefaultStream(outputStreamName);
        sink.setDefaultPartition("0");
        return sink;
    }

    private static void RunStreamingAggregator() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = createSourceFromStaticConfig(env);
        SingleOutputStreamOperator<Object> d= input
                .map(value -> TransformPod(value));
        //SingleOutputStreamOperator<Object> d= input.map(value -> value);
        d.print("henry");
        //input.map(value -> TransformPod(value)).addSink(createSinkFromStaticConfig());
        env.execute("kubernetes-job-podcost-aggreagator");
    }

    static final ReduceFunction<PodMessageRaw> mazuKubeReducer = new ReduceFunction<PodMessageRaw>(){ //Window Functions - one of ReduceFunction, AggregateFunction, FoldFunction or ProcessWindowFunction
        @Override
        public PodMessageRaw reduce(PodMessageRaw p1, PodMessageRaw p2) throws Exception {
            PodMessageRaw.Builder pmb = PodMessageRaw.newBuilder(p1);
            return pmb.setCpuRequested(1).setJobName("henry.wu").build();
        }};

    private static void RunStreamingAggregator2() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().registerTypeWithKryoSerializer(PodMessageRaw.class, ProtobufSerializer.class);
        Class<?> unmodColl = Class.forName("java.util.Collections$UnmodifiableCollection");
        env.getConfig().addDefaultKryoSerializer(unmodColl, UnmodifiableCollectionsSerializer.class);
        DataStream<String> input = createSourceFromStaticConfig(env);

        //SingleOutputStreamOperator<Object> d= input
        SingleOutputStreamOperator<String> d= input
                //KeyedStream<PodMessageRaw, String> d = input
                .map(value -> TransformPod2(value))
                .keyBy(new JobNameKeySelector()) // Keyed Windows
                //pre-defined window assigners for the most common use cases, namely tumbling windows, sliding windows, session windows and global windows.
                //.window(EventTimeSessionWindows.withGap(Time.seconds(15)))
                //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .reduce(mazuKubeReducer)
                .map(v -> v.toString());
        //SingleOutputStreamOperator<Object> d= input.map(value -> value);

        d.print("henry");
        //input.map(value -> TransformPod(value)).addSink(createSinkFromStaticConfig());
        env.execute("kubernetes-job-podcost-aggreagator");
    }

    private static void RunStreamingAggregator_Sliding() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.getConfig().registerTypeWithKryoSerializer(PodMessageRaw.class, ProtobufSerializer.class);
        Class<?> unmodColl = Class.forName("java.util.Collections$UnmodifiableCollection");
        env.getConfig().addDefaultKryoSerializer(unmodColl, UnmodifiableCollectionsSerializer.class);
        DataStream<String> input = createSourceFromStaticConfig(env);

        SingleOutputStreamOperator<String> d= input
                .map(value -> TransformPod2(value))
                .keyBy(new JobNameKeySelector()) // Keyed Windows
                .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(10)))
                .reduce(mazuKubeReducer)
                .map(v -> ComputeRaw(v.toString()).toString());
        //SingleOutputStreamOperator<Object> d= input.map(value -> value);

        d.print("henry");
        //input.map(value -> TransformPod(value)).addSink(createSinkFromStaticConfig());
        env.execute("kubernetes-job-podcost-aggreagator");
    }


    private static void RunLocalStreamingAggregator() throws Exception {
        final StreamExecutionEnvironment env = LocalStreamEnvironment.getExecutionEnvironment();
        DataStream<String> input = createSourceFromStaticConfig(env);
        ObjectMapper jsonParser = new ObjectMapper();
        input.map(value -> TransformPod(value)).addSink(createSinkFromStaticConfig());
        env.execute("kubernetes-job-cost-aggreagator");
    }

    // TODO: the transform calculation function should be added to this function!
    // 1. calculation 2. convert back to String
    private static String TransformPod(String d1) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        Message.Builder pb = PodMessageRaw.newBuilder();
        PodMessageRaw.Builder pmb = PodMessageRaw.newBuilder();
        JsonFormat.parser().merge(d1, pmb);
        PodMessageRaw m = pmb.setAsset("ass").build(); // m is PodMessageRaw
        // calculation here
        return m.toString();
    }

    private static PodMessageRaw TransformPod2(String d1) throws Exception {
        try {
            final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
            Message.Builder pb = PodMessageRaw.newBuilder();
            PodMessageRaw.Builder pmb = PodMessageRaw.newBuilder();
            JsonFormat.parser().merge(d1, pmb);
            PodMessageRaw m = pmb.setAsset("ass").build(); // m is PodMessageRaw
            // calculation here
            return m;
        }
        catch(com.google.protobuf.InvalidProtocolBufferException e) {
            System.out.println(d1);
            return  PodMessageRaw.newBuilder().build();
        }
    }

    // calculatePodCost calculates the cost of a pod
    // Cost = (RESOURCE_REQUESTED / (INSTANCE_CAPACITY * 1000))  * INSTANCE_HOURLY_COST * POD_DURATION_IN_MINUTES
    private static PodMessageEnriched ComputeRaw(String d1) throws Exception {
        try {
            final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
            Message.Builder pb = PodMessageRaw.newBuilder();
            PodMessageRaw.Builder pmb = PodMessageRaw.newBuilder();
            JsonFormat.parser().merge(d1, pmb);
            PodMessageRaw m = pmb.build(); // m is PodMessageRaw
            // calculation here
            double cpuCost = 0.0;
            double gpuCost = 0.0;
            double memoryCost = 0.0;
            final long memBytesRequested = m.getMemoryRequested();
            final long durationInMinute = (long)Math.ceil((m.getEndTime() - m.getStartTime())/60.);
        	/*if (memBytesRequested>0) {
        		memoryCost = (memBytesRequested/ (ec2Instance.MemoryBytesCapacity)) * (ec2Instance.HourlyPrice / 60) * durationInMinute;
        	}*/

            PodMessageEnriched.Builder out = PodMessageEnriched.newBuilder();
            return out.build();
        }
        catch(com.google.protobuf.InvalidProtocolBufferException e) {
            System.out.println(d1);
            return  PodMessageEnriched.newBuilder().build();
        }
    }

    // https://stackoverflow.com/questions/32453030/using-an-collectionsunmodifiablecollection-with-apache-flink
    public static void main(String[] args) throws Exception {
        //RunLocalStreamingAggregator();
        //test();
        //RunStreamingAggregator_Sliding();
        RunStreamingAggregator();
    }

    private static class JobNameKeySelector
            implements KeySelector<PodMessageRaw, String> {
        @Override
        public String getKey(PodMessageRaw value) {
            return value.getJobName();
        }
    }
}