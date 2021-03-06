# AffinityStreamApp
Kafka Stream for processing consumer affinity based on his actions

# Embeded Server:
url= http://10.100.1.17:7003  
changeLog: PUT /logger `{"logName": "com.nyble", "logLevel": "warn"}`

# Installation
#### check that actions rmc/rrp and subcampaignes kafka connectors are paused and affinity consumers have lag = 0

#### check that stream app AffinityStreamApp.jar is not running

#### get the latest id from actions in database:
```sql
update config_parameters  
set value = q.id  
from (select max(id) as id from consumer_actions where system_id = 1) q  
where key = 'AFFINITY_LAST_ACTION_ID_RMC';--lastRmcActionId
create temp table tmp as select id from consumer_actions ca where system_id = 2;
update config_parameters  
set value = q.id  
from (select max(id) as id from tmp) q  
where key = 'AFFINITY_LAST_ACTION_ID_RRP';--lastRmcActionId  
drop table tmp;
```
```shell script
cd /home/crmsudo/jobs/kafkaClients/scripts && node utility.js --get-script \
name=affinity replace=:lastRmcActionId^TODO replace=:lastRrpActionId^TODO
```

#### delete internal stream app topics
`~/kits/confluent-5.5.1/bin/kafka-topics --bootstrap-server 10.100.1.17:9093  --delete --topic <everything starting with affinity-stream-....>`

#### load initial state
```shell script
PGPASSWORD=postgres10@ nohup psql -U postgres -h localhost -d datawarehouse \
-f "/home/crmsudo/jobs/kafkaClients/scripts/initial-affinity-calc.sql"&
```
```shell script
PGPASSWORD=postgres10@ nohup psql -U postgres -h localhost -d datawarehouse \
-c "\\copy (select 
replace(replace(json_build_object('systemId', system_id, 'consumerId', consumer_id)::text, ' : ', ':'), ', ', ','),
replace(replace(json_build_object('systemId', system_id::text, 'consumerId', consumer_id::text, 
    'key', concat('affinity_', brand_id), 'value', score::text, 
    'externalSystemDate', round(extract(epoch from now())*1000)::text,  
    'localSystemDate', round(extract(epoch from now())*1000)::text)::text, ' : ', ':'), ', ', ',')
from consumers_score_start) to '/tmp/affinity.csv' delimiter ';' " &
```  
```shell script
cd /home/crmsudo/jobs/scripts/kafkaToolsJava
./kafkaTools.sh producer --topic consumer_attributes --bootstrap-server 10.100.1.17:9093 \
--value-serializer String --key-serializer String --format key-value --key-value-delimiter ";" \
--file /tmp/affinity.csv
```

#### start streams app