# GCP_UseCase1_Durga
# Steps:
1. git clone https://github.com/Durga-Yelmame/GCP_UseCase1_Durga
2. cd GCP_UseCase1_Durga
3. export LAB_ID=19..... mention your Lab-ID
4. mvn compile exec:java -Dexec.mainClass=UseCase1_PubSub_to_BigQuery -Dexec.args="--project=nttdata-c4e-bde \
--stagingLocation=gs://c4e-uc1-dataflow-temp-$LAB_ID/staging \
--region=europe-west4 --serviceAccount=c4e-uc1-sa-$LAB_ID@nttdata-c4e-bde.iam.gserviceaccount.com \
--maxNumWorkers=1 --workerMachineType=n1-standard-1 --gcpTempLocation=gs://c4e-uc1-dataflow-temp-$LAB_ID/temp \
--runner=DataflowRunner --jobName=usecase1-labid-$LAB_ID --subnetwork=regions/europe-west4/subnetworks/subnet-uc1-$LAB_ID \
--inputTopic=uc1-input-topic-$LAB_ID --tableName=uc1_$LAB_ID.account --subscription=uc1-input-topic-sub-19 --dlqTopic=uc1-dlq-topic-19 \
--streaming"
5. Go to PubSub Input Topic and Publish Message
6. Message format : {"id": N, "name": "name1", "surname": "name2"}
   Eg: {"id": 1, "name": "Durga", "surname": "Yelmame"} ---> Valid Data ---> Added to Table
   {"id": A1, "name": "Sanjay", "surname": "Kapoor"} -----> InValid data ----> send to DLQ topic
7. Check Table for valid data-> SELECT * FROM `nttdata-c4e-bde.uc1_19.account` LIMIT 1000
8. For Invalid data -> go to DLQ topic Subscription and Messages -> PULL. 


