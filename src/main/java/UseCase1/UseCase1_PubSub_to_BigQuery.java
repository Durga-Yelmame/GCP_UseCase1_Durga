package UseCase1;

import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.options.Description;

public class UseCase1_PubSub_to_BigQuery {

    private static TupleTag<CommonLog> VALID_DATA=new TupleTag<CommonLog>(){};
    private static TupleTag<String> INVALID_DATA=new TupleTag<String>(){};
    private static final Logger LOG = LoggerFactory.getLogger(UseCase1_PubSub_to_BigQuery.class);


    /*
    Pub/Sub topic name, Table name, topic subscription and DLQ topic are parametric.
    */
    public interface PipelineOptions extends DataflowPipelineOptions {
        @Description("Input topic name")
        String getInputTopic();
        void setInputTopic(String inputTopic);

        @Description("BigQuery table name")
        String getTableName();
        void setTableName(String tableName);

        @Description("input Subscription of PubSub")
        String getSubscription();
        void setSubscription(String subscription);

        @Description("DLQ topic of PubSub")
        String getDlqTopic();
        void setDlqTopic(String dlqTopic);
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(PipelineOptions.class);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);
        run(options);
    }


    static class JsonToCommonLog extends DoFn<String, CommonLog> {
        @ProcessElement
        public void processElement(@Element String json, OutputReceiver<CommonLog> r,ProcessContext processContext) throws Exception {
            try {
                Gson gson = new Gson();
                CommonLog commonLog = gson.fromJson(json, CommonLog.class);
                processContext.output(VALID_DATA,commonLog);
            }catch(Exception e){
                processContext.output(INVALID_DATA,json);
            }
        }
    }
    /*
    1. Piepline will Read Data from PubSub Subscription.
    2. Transform Data
    3. Extract Valid Data and Invalid Data.
    4. Valid Data will be written in BigQuery Table
    5. Invalid Data will be send to DLQ Topic
    */
    public static PipelineResult run(PipelineOptions options) {
        String PubSubsubscriptionName="projects/"+options.getProject()+"/subscriptions/"+options.getSubscription();
        String outputTableName=options.getProject()+":"+options.getTableName();
        String dlqTopicName="projects/"+options.getProject()+"/topics/"+options.getDlqTopic();
        options.setJobName(options.getJobName());

        Pipeline pipeline = Pipeline.create(options);

        PCollectionTuple rowCheck = pipeline
                .apply("ReadMessageFromPubSub", PubsubIO.readStrings()
                        .fromSubscription(PubSubsubscriptionName))


                .apply("ParseJson", ParDo.of(new JsonToCommonLog()).withOutputTags(VALID_DATA, TupleTagList.of(INVALID_DATA)));


        PCollection<CommonLog> validData=rowCheck.get(VALID_DATA);
        PCollection<String> invalidData=rowCheck.get(INVALID_DATA);


        validData.apply("WriteDataToTable",
                BigQueryIO.<CommonLog>write().to(outputTableName).useBeamSchema()
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));


        invalidData.apply("SendInValidDataToDLQ",PubsubIO.writeStrings().to(dlqTopicName));
        LOG.info("Building pipeline...");
        return pipeline.run();

    }
}
