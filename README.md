# AffinityStreamApp
Kafka Stream for processing consumer affinity based on his actions

# Installation
-- THIS PORTION IS ONLY MANDATORY ON RECALCULATIONS --

cd ~/kits/confluent-5.5.1/

--get the latest id from actions in database:
select max(id) from consumer_actions where system_id = 1; --lastRmcActionId
select max(id) from consumer_actions where system_id = 2; --lastRrpActionId
--update ids in the application and build it
--update ids in the initial-affinity-calc.sql
PGPASSWORD=postgres10@ nohup psql -U postgres -h localhost -d datawarehouse -f "/home/crmsudo/jobs/kafkaClients/scripts/initial-affinity-calc.sql"&

--check that stream app AffinityStreamApp.jar is not running

--check that kafka connector jdbc_source_consumer_affinity_score_start does not exist OR pause and delete it
--reset kafka connector jdbc_source_consumer_affinity_score_start offset
./bin/kafka-console-producer --bootstrap-server 10.100.1.17:9093 --topic connect-consumer-action-offsets --property "parse.key=true" --property "key.separator=;"
["jdbc_source_consumer_affinity_score_start",{"query":"query"}];{"incrementing":0}

--empty intermediate topic intermediate-affinity-scores:
bin/kafka-configs --bootstrap-server 10.100.1.17:9093 --alter --entity-type topics --entity-name intermediate-affinity-scores --add-config retention.ms=10
--wait
bin/kafka-configs --bootstrap-server 10.100.1.17:9093 --alter --entity-type topics --entity-name intermediate-affinity-scores --delete-config retention.ms

--use reset for affinity-stream stream app TODO:
./bin/kafka-streams-application-reset --application-id affinity-stream --input-topics affinity-actions --bootstrap-servers 10.100.1.17:9093
./bin/kafka-streams-application-reset --application-id affinity-stream --input-topics affinity-actions,subcampaignes --intermediate-topics intermediate-affinity-scores --bootstrap-servers 10.100.1.17:9093

--delete internal stream app topics
./bin/kafka-topics --bootstrap-server 10.100.1.17:9093  --delete --topic <everything starting with affinity-stream-....>

--start streams app

--create kafka connector jdbc_source_consumer_affinity_score_start and wait until full table is loaded
--pause and delete kafka connector jdbc_source_consumer_affinity_score_start
--delete kafka connector jdbc_source_consumer_affinity_score_start
