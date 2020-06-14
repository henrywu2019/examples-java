package io.github.streamingwithflink.mazu;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.dynamodbv2.model.Get;
import org.apache.http.entity.StringEntity;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import podmsgenriched.MazuRecordEnriched;
import podmsgraw.MazuRecordRaw.PodMessageRaw;
import podmsgenriched.MazuRecordEnriched.PodMessageEnriched;

import java.io.IOException;
import java.util.*;

//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import akka.remote.serialization.ProtobufSerializer;

// https://code.uberinternal.com/diffusion/ATATGXS/browse/master/handler/batch_kubernetes_collector.go$406
public class StreamAggregator {
    final private static String region = "us-east-1";
    final private static String inputStreamName = "mazu-kubernetes-podwatcher-raw";
    final private static String outputStreamName = "mazu-kubernetes-podwatcher-enriched";
    final private static String pricingEndpoint = "http://pricing.aws.uberatc.net/pricing";
    final private static String taggingEndpoint = "http://mazutagging.aws.uberatc.net/%s";
    final static private Map<String, String> taggingMap = new HashMap<>();

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        String timestamp = "2020-06-14T01:01:01.480-00:00";
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, timestamp);
        inputProperties.setProperty(ConsumerConfigConstants.AWS_PROFILE_NAME, "UberATGProd/ATGEng");
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
        final FlinkKinesisProducer<String> sink = new FlinkKinesisProducer<>(new SimpleStringSchema(), outputProperties);
        sink.setDefaultStream(outputStreamName);
        sink.setDefaultPartition("0");
        return sink;
    }

    private static void RunStreamingAggregator() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = createSourceFromStaticConfig(env);
        //SingleOutputStreamOperator<Object> d= input.map(value -> TransformPod(value));
        //SingleOutputStreamOperator<Object> d= input.map(value -> value);
        //d.print("henry");
        input.map(StreamAggregator::TransformPod).addSink(createSinkFromStaticConfig());
        env.execute("kubernetes-job-podcost-aggreagator");
    }

    // TODO: the transform calculation function should be added to this function!
    // 1. calculation 2. convert back to String
    private static String TransformPod(String d1) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        PodMessageRaw.Builder pmb = PodMessageRaw.newBuilder();
        PodMessageEnriched.Builder ob = MazuRecordEnriched.PodMessageEnriched.newBuilder();
        JsonFormat.parser().merge(d1, pmb);
        if (pmb.getEndTime() == -1) {
            return "";
        }

        // Uber Tagging
        Map<String, String> m = pmb.getLabelsMap();
        String owner = m.get("uber.com/owner");
        String tags = "";
        if (taggingMap.containsKey(owner)) {
            tags = taggingMap.get(owner);
        } else {
            tags = GetTags(owner + "@uber.com");
        }
        if (!tags.isEmpty()) {
            taggingMap.put(owner, tags);
        }
        //System.out.println(tags);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode neoJsonNode = mapper.readTree(tags);
        for (JsonNode n : neoJsonNode) {
            String k = n.get("Key").asText();
            String v = n.get("Value").asText();
            switch (k) {
                case "Uber Organization":
                    ob.setOrganization(v);
                    break;
                case "Uber Owner":
                    ob.setOwner(v);
                    break;
                case "ATG SLT Owner":
                    ob.setAtgslt(v);
                    break;
                case "Uber Team":
                    ob.setTeam(v);
                    break;
            }
        }

        // Cost
        // curl -X POST -d '{"node":"x","mem":60000000000,"cpu":16000,"gpu":0,"model":"spot","instance":"r5a.16xlarge","duration":123}' http://pricing.aws.uberatc.net/pricing
        //{"mem_cost":0.8090282790362835,"cpu_cost":1.8532000000000002,"gpu_cost":0,"dom_cost":1.8532000000000002}
        final String pricing_model = pmb.getLabelsOrDefault("pricing_model", "");
        final String instance_type = pmb.getLabelsOrDefault("instance_type", "");
        final String environment = pmb.getLabelsOrDefault("env", "production");
        if (environment.equals("production") && !pricing_model.equals("") && !instance_type.equals("")) {
            final String pricing = GetPricing(pmb.getMemoryRequested(),
                    pmb.getCpuRequested(),
                    pmb.getGpuRequested(),
                    pricing_model,
                    instance_type,
                    pmb.getEndTime() - pmb.getStartTime());
            System.out.println("pricing:" + pricing);
            // {"mem_cost":0.8486509323120118,"cpu_cost":0.8898750000000001,"gpu_cost":0,"dom_cost":0.8898750000000001}
            neoJsonNode = mapper.readTree(pricing);
            System.out.println(neoJsonNode.getNodeType());
            ob.setMemoryCost(Double.parseDouble(neoJsonNode.get("mem_cost").toString()));
            ob.setCpuCost(Double.parseDouble(neoJsonNode.get("cpu_cost").toString()));
            ob.setGpuCost(Double.parseDouble(neoJsonNode.get("gpu_cost").toString()));
            ob.setDominantCost(Double.parseDouble(neoJsonNode.get("dom_cost").toString()));
        }


        //PodMessageRaw r = pmb.setPodName("ass").build(); // m is PodMessageRaw
        ob.setPodName(pmb.getPodName());
        ob.setJobName(pmb.getLabelsOrDefault("uber.com/resource-name", ""));
        ob.setStartTime(pmb.getStartTime());
        ob.setEndTime(pmb.getEndTime());
        ob.setAsset(pmb.getLabelsOrDefault("uber.com/asset", ""));
        ob.setInstanceType(pmb.getLabelsOrDefault("instance-type", ""));
        ob.setPricingModel(pmb.getLabelsOrDefault("pricing-model", ""));
        ob.setCluster(pmb.getLabelsOrDefault("cluster", ""));
        ob.setInstanceType(pmb.getLabelsOrDefault("ingestion_time", ""));
        if (ob.getOwner().isEmpty()) {
            ob.setOwner(pmb.getLabelsOrDefault("uber.com/owner", ""));
        }

        final String result = ob.build().toString();
        return result;
    }

    public static void test() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode neoJsonNode = mapper.readTree("[{\"Value\": \"ATG Software\", \"Key\": \"Uber Organization\"},\n" +
                "    {\"Value\": \"mphillips@uber.com\", \"Key\": \"Uber Owner\"},\n" +
                "    {\"Value\": \"tstentz@uber.com\", \"Key\": \"ATG SLT Owner\"},\n" +
                "    {\"Value\": \"ATG-Motion Planning-Vehicle Behaviors\", \"Key\": \"Uber Team\"}]");
        String nt = neoJsonNode.getNodeType().name();
        for (JsonNode n : neoJsonNode) {
            String k = n.get("Key").asText();
            String v = n.get("Value").asText();
            switch (k) {
                case "Uber Organization":
                    System.out.println("Monday");
                    break;
                case "Uber Owner":
                    System.out.println("Tuesday");
                    break;
                case "ATG SLT Owner":
                    System.out.println("Wednesday");
                    break;
                case "Uber Team":
                    System.out.println("Thursday");
                    break;
            }
        }
    }

    // [{"Value": "ATG Software", "Key": "Uber Organization"},
    //{"Value": "mphillips@uber.com", "Key": "Uber Owner"},
    //{"Value": "tstentz@uber.com", "Key": "ATG SLT Owner"},
    //{"Value": "ATG-Motion Planning-Vehicle Behaviors", "Key": "Uber Team"}]
    public static String GetTags(String email)
            throws ClientProtocolException, IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(String.format(taggingEndpoint, email));
        CloseableHttpResponse response = client.execute(httpPost);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_OK) {
            final HttpEntity entity = response.getEntity();
            try {
                return entity != null ? EntityUtils.toString(entity) : null;
            } catch (final ParseException ex) {
                throw new ClientProtocolException(ex);
            }
        }
        client.close();
        return "";
    }


    // https://howtodoinjava.com/library/jaxrs-client-httpclient-get-post/
    public static String GetPricing(Long mem, Long cpu, Long gpu, String model, String instance, Long duration)
            throws ClientProtocolException, IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(pricingEndpoint);

        // GET
        /*List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("node", "x"));
        params.add(new BasicNameValuePair("mem", mem.toString()));
        params.add(new BasicNameValuePair("cpu", cpu.toString()));
        params.add(new BasicNameValuePair("gpu", gpu.toString()));
        params.add(new BasicNameValuePair("model", model));
        params.add(new BasicNameValuePair("instance", instance));
        params.add(new BasicNameValuePair("duration", duration.toString()));
        httpPost.setEntity(new UrlEncodedFormEntity(params));*/

        final StringEntity inputEntity = new StringEntity(String.format("{\"node\":\"x\",\"mem\":%d,\"cpu\":%d," +
                        "\"gpu\":%d,\"model\":\"%s\",\"instance\":\"%s\",\"duration\":%d}",
                mem, cpu, gpu, model, instance, duration));
        httpPost.addHeader("content-type", "application/json");
        httpPost.setEntity(inputEntity);

        CloseableHttpResponse response = client.execute(httpPost);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_OK) {
            final HttpEntity entity = response.getEntity();
            try {
                return entity != null ? EntityUtils.toString(entity) : null;
            } catch (final ParseException ex) {
                throw new ClientProtocolException(ex);
            }
        }
        client.close();
        return "";
    }

    // https://stackoverflow.com/questions/32453030/using-an-collectionsunmodifiablecollection-with-apache-flink
    public static void main(String[] args) throws Exception {
        RunStreamingAggregator();
    }
}