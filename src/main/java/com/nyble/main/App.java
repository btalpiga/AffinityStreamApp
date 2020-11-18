package com.nyble.main;

import com.google.gson.Gson;
import com.nyble.exceptions.RuntimeSqlException;
import com.nyble.managers.ProducerManager;
import com.nyble.streams.types.BrandAffinityValue;
import com.nyble.streams.types.SubcampaignesKey;
import com.nyble.streams.types.SubcampaignesValue;
import com.nyble.streams.types.SystemConsumerBrand;
import com.nyble.topics.Names;
import com.nyble.topics.TopicObjectsFactory;
import com.nyble.topics.consumerActions.ConsumerActionsKey;
import com.nyble.topics.consumerActions.ConsumerActionsValue;
import com.nyble.topics.consumerAttributes.ConsumerAttributesKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesValue;
import com.nyble.util.DBUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SpringBootApplication(scanBasePackages = {"com.nyble.rest"})
public class App {

    final static String KAFKA_CLUSTER_BOOTSTRAP_SERVERS = "10.100.1.17:9093";
    final static Logger logger = LoggerFactory.getLogger(App.class);
    final static String appName = "affinity-stream";
    final static Properties streamsConfig = new Properties();
    final static Properties producerProps = new Properties();
    static ProducerManager producerManager;
    static{
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        streamsConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        streamsConfig.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");

        producerProps.put("bootstrap.servers", KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        producerProps.put("acks", "all");
        producerProps.put("retries", 5);
        producerProps.put("batch.size", 16384);
        producerProps.put("linger.ms", 1);
        producerProps.put("buffer.memory", 33554432);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerManager = ProducerManager.getInstance(producerProps);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            producerManager.getProducer().flush();
            producerManager.getProducer().close();
        }));
    }

    public static void initSourceTopic(List<String> topics) throws ExecutionException, InterruptedException {
        //this values should be updated on redeployment, if actions are reloaded
        int lastRmcActionId;
        int lastRrpActionId;
        final String configName = "AFFINITY_LAST_ACTION_ID_%";
        final String lastActIdsQ = String.format("select vals[1]::int as rmc, vals[2]::int as rrp from\n" +
                "(\tselect array_agg(value order by key) as vals \n" +
                "\tfrom config_parameters cp \n" +
                "\twhere key like '%s'\n" +
                ") foo", configName);
        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(lastActIdsQ)){
            if(rs.next()){
                lastRmcActionId = rs.getInt("rmc");
                lastRrpActionId = rs.getInt("rrp");
            }else{
                throw new RuntimeException("Last action ids not set");
            }
        } catch (SQLException e) {
            throw new RuntimeSqlException(e.getMessage(), e);
        }
        //
        final String consumerGroupId = "affinity-source-creator";
        final Gson gson = new Gson();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000*60);

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        AdminClient adminClient = AdminClient.create(adminProps);
        Set<String> existingTopics = adminClient.listTopics().names().get();
        for(String checkTopic : topics){
            if(!existingTopics.contains(checkTopic)){
                int numPartitions = 4;
                short replFact = 2;
                NewTopic st = new NewTopic(checkTopic, numPartitions, replFact).configs(new HashMap<>());
                adminClient.createTopics(Collections.singleton(st));
            }
        }

        String sourceTopic = topics.get(0);
        Thread poolActionsThread = new Thread(()->{
            KafkaConsumer<String, String> kConsumer = new KafkaConsumer<>(consumerProps);
            kConsumer.subscribe(Arrays.asList(Names.CONSUMER_ACTIONS_RMC_TOPIC, Names.CONSUMER_ACTIONS_RRP_TOPIC));
            while(true){
                ConsumerRecords<String, String> records = kConsumer.poll(Duration.ofSeconds(10));
                records.forEach(record->{
                    String provenienceTopic = record.topic();
                    int lastAction;
                    if(provenienceTopic.endsWith("rmc")){
                        lastAction = lastRmcActionId;
                    } else if(provenienceTopic.endsWith("rrp")){
                        lastAction = lastRrpActionId;
                    } else {
                        return;
                    }

                    //filter actions
                    ConsumerActionsValue cav = (ConsumerActionsValue) TopicObjectsFactory.fromJson(record.value(), ConsumerActionsValue.class);
                    if(Integer.parseInt(cav.getId()) > lastAction && AffinityActionsDict.filter(cav)){
                        ConsumerActionsValue.ConsumerActionsPayload consumerActionPayload = cav.getPayloadJson();
                        if(consumerActionPayload!= null && consumerActionPayload.subcampaignId != null){
                            int subcampaignId = Integer.parseInt(consumerActionPayload.subcampaignId);
                            int systemId = Integer.parseInt(cav.getSystemId());
                            String skAsString = gson.toJson(new SubcampaignesKey(systemId, subcampaignId));
                            producerManager.getProducer().send(new ProducerRecord<>(sourceTopic, skAsString, record.value()));
                            logger.debug("Create {} topic input",sourceTopic);
                        }
                    }
                });
            }
        });
        poolActionsThread.start();
    }

    public static void scheduleBatchUpdate(String intermediateTopic){
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final Runnable task = ()->{
            try {
                logger.info("Start update ");
                updateScoresEveryHour(intermediateTopic);
                logger.info("End update");
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        };
        Duration delay = Duration.between(Instant.now(), Instant.now().plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS));
        scheduler.scheduleAtFixedRate(task, delay.toMillis(), Duration.ofHours(1).toMillis(), TimeUnit.MILLISECONDS);
        logger.info("Task will start in: "+delay.toMillis()+" millis");

        Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdown));
    }

    public static void main(String[] args){

        try{
            SpringApplication.run(App.class, args);
            final String sourceTopic = "affinity-actions";
            final String subcampaignesTopic = "subcampaignes";
            final String intermediateTopic = "intermediate-affinity-scores";

            initSourceTopic(Arrays.asList(sourceTopic, intermediateTopic));
            scheduleBatchUpdate(intermediateTopic);


            final Gson gson = new Gson();
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> affinityEvents = builder
                    .stream(sourceTopic,Consumed.with(Serdes.String(), Serdes.String())
                    );
            final KTable<String, String> subcampaignesTable = builder.table(subcampaignesTopic,
                    Consumed.with(Serdes.String(), Serdes.String()));

            final KStream<String, String> actionsEventsEnrichedWithSubcampaignes = affinityEvents
                    .join(subcampaignesTable,(action, subcampaign)->{
                        logger.debug("Join action with subcampaign");
                        ConsumerActionsValue consumerActionsValue = (ConsumerActionsValue) TopicObjectsFactory
                                .fromJson(action, ConsumerActionsValue.class);
                        ConsumerActionsValue.ConsumerActionsPayload consumerActionPayload = consumerActionsValue.getPayloadJson();
                        int actionScore = AffinityActionsDict.getScore(consumerActionsValue.getSystemId()+"",
                                consumerActionsValue.getActionId()+"", consumerActionPayload);
                        SubcampaignesValue subcampaignesValue = gson.fromJson(subcampaign, SubcampaignesValue.class);
                        int brandId = subcampaignesValue.getBrandId();
                        //allowed brands are //117 = CAMEL   125 = SOBRANIE   127 = WINSTON   138 = LOGIC   486 = Multiref.brand
                        if(brandId == 13 && consumerActionsValue.getSystemId().equals("1")){
                            brandId = 138;
                        }else if(brandId == 486){
                            if(consumerActionPayload.getValue() == null){
                                brandId = -1;
                            }else{
                                String skuBought = consumerActionPayload.getValue().sku_bought!= null ?
                                        consumerActionPayload.getValue().sku_bought : "";
                                if(skuBought.toUpperCase().startsWith("SB") || skuBought.toUpperCase().startsWith("SO")){brandId = 125;}
                                else if(skuBought.toUpperCase().startsWith("WI")){brandId = 127;}
                                else if(skuBought.toUpperCase().startsWith("CA")){brandId = 117;}
                                else if(skuBought.toUpperCase().startsWith("LG")){brandId = 138;}
                                else {brandId = -1;}
                            }
                        }
                        BrandAffinityValue bav = new BrandAffinityValue(Integer.parseInt(consumerActionsValue.getSystemId()),
                                Integer.parseInt(consumerActionsValue.getConsumerId()), brandId, actionScore);
                        return gson.toJson(bav);
                    },Joined.with(Serdes.String(), Serdes.String(), Serdes.String()))
                    .map((subcampaignStr, bavStr)->{
                        logger.debug("Remap key from subcampaign to consumer");
                        SubcampaignesKey sk = gson.fromJson(subcampaignStr, SubcampaignesKey.class);
                        BrandAffinityValue bav = gson.fromJson(bavStr, BrandAffinityValue.class);
                        ConsumerActionsKey cak = new ConsumerActionsKey(sk.getSystemId(), bav.getConsumerId());
                        return KeyValue.pair(cak.toJson(), bavStr);
                    })
                    .filter((consumerStr, bavStr) -> {
                        logger.debug("Filter out actions with score == 0 and brand not allowed");
                        BrandAffinityValue bav = gson.fromJson(bavStr, BrandAffinityValue.class);
                        return bav != null && (bav.getDeltaScore() != 0)
                                && AffinityActionsDict.allowedBrands.contains(bav.getBrandId());
                    });

            actionsEventsEnrichedWithSubcampaignes
                    .map((consumerActionsKeyStr, bavStr) ->{
                        logger.debug("Remap key including brand from subcampaign");
                        ConsumerActionsKey cak = (ConsumerActionsKey) TopicObjectsFactory
                                .fromJson(consumerActionsKeyStr, ConsumerActionsKey.class);
                        BrandAffinityValue bav = gson.fromJson(bavStr, BrandAffinityValue.class);
                        return KeyValue.pair(
                                gson.toJson(new SystemConsumerBrand(cak.getSystemId(),cak.getConsumerId(),bav.getBrandId())),bavStr
                        );
                    })
                    .through(intermediateTopic, Produced.with(Serdes.String(), Serdes.String()))
                    .groupByKey()
                    .reduce((bavStr1, bavStr2)->{
                        BrandAffinityValue bav1 = gson.fromJson(bavStr1, BrandAffinityValue.class);
                        BrandAffinityValue bav2 = gson.fromJson(bavStr2, BrandAffinityValue.class);
                        bav2.add(bav1.getDeltaScore());
                        return gson.toJson(bav2);
                    })
                    .toStream()
                    .map((scbStr, bavStr)->{
                        String currentTime = new Date().getTime()+"";
                        BrandAffinityValue bav = gson.fromJson(bavStr, BrandAffinityValue.class);
                        ConsumerAttributesKey cak = new ConsumerAttributesKey(bav.getSystemId(), bav.getConsumerId());
                        ConsumerAttributesValue cav = new ConsumerAttributesValue(bav.getSystemId()+"", bav.getConsumerId()+"",
                                "affinity_"+bav.getBrandId(), bav.getDeltaScore()+"", currentTime, currentTime);
                        return KeyValue.pair(cak.toJson(), cav.toJson());
                    })
                    .to(Names.CONSUMER_ATTRIBUTES_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

            Topology topology = builder.build();
            logger.debug(topology.describe().toString());
            KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
            streams.cleanUp();
            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }catch(Exception e){
            logger.error(e.getMessage(), e);
            logger.error("EXITING");
            System.exit(1);
        }
    }


    private static void updateScoresEveryHour(String intermediateTopic) throws Exception {
        Gson gson = new Gson();
        Map<SystemConsumerBrand, BrandAffinityValue> decrements = new HashMap<>();
        final String startDate = getLastDate();
        Calendar now = new GregorianCalendar();
        now.add(Calendar.YEAR, -2);
        now.set(Calendar.MILLISECOND, 0);
        now.set(Calendar.SECOND, 0);
        now.set(Calendar.MINUTE, 0);
        final String endDate = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(now.getTime());

        logger.info("Remove score from {} to {}", startDate, endDate);
        final String query = "select ca.system_id, ca.consumer_id, r.brand_id , ca.action_id, ca.payload_json->>'value' as value, \n" +
                "(ca.payload_json ->>'subcampaignId')::int, ca.payload_json::text as payload\n" +
                "from consumer_actions ca \n" +
                "join recoded_subcampaignes r on (ca.payload_json ->>'subcampaignId')::int = r.id and ca.system_id = r.system_id\n" +
                "where ca.external_system_date > '"+startDate+"' and ca.external_system_date <= '"+endDate+"' and \n" +
                "r.brand_id in (13,"+AffinityActionsDict.allowedBrands.stream().map(bId->bId+"").collect(Collectors.joining(","))+")";
        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query)){
            st.setFetchSize(1000);
            while(rs.next()){
                int systemId = rs.getInt(1);
                int consumerId = rs.getInt(2);
                int brandId = rs.getInt(3);
                int actionId = rs.getInt(4);
                String jsonValue = rs.getString(5);
                int subcampaignId = rs.getInt(6);
                String payload = rs.getString(7);

                ConsumerActionsValue.ConsumerActionsPayload.Value json = gson.fromJson(jsonValue,
                        ConsumerActionsValue.ConsumerActionsPayload.Value.class);
                ConsumerActionsValue.ConsumerActionsPayload cap = new ConsumerActionsValue.ConsumerActionsPayload(
                        subcampaignId+"", null, null, null, null, null,
                        null, null, null);
                cap.setValue(json);
                cap.setRaw(payload);
                //allowed brands are //117 = CAMEL   125 = SOBRANIE   127 = WINSTON   138 = LOGIC   486 = Multiref.brand
                if(brandId == 13 && systemId == 1){
                    //this is Logic
                    brandId = 138;
                }else if(brandId == 486){
                    if(json == null){
                        //brandId = -1;
                        continue;
                    }else{
                        String skuBought = json.sku_bought != null ? json.sku_bought : "";
                        if(skuBought.toUpperCase().startsWith("SB") || skuBought.toUpperCase().startsWith("SO")){brandId = 125;}
                        else if(skuBought.toUpperCase().startsWith("WI")){brandId = 127;}
                        else if(skuBought.toUpperCase().startsWith("CA")){brandId = 117;}
                        else if(skuBought.toUpperCase().startsWith("LG")){brandId = 138;}
                        else {continue;}
                    }
                }
                SystemConsumerBrand scb = new SystemConsumerBrand(systemId, consumerId, brandId);
                int actionScore = AffinityActionsDict.getScore(systemId+"", actionId+"", cap);
                if(actionScore != 0){
                    BrandAffinityValue bav = decrements.get(scb);
                    if(bav != null){
                        bav.add(actionScore);
                    }else{
                        bav = new BrandAffinityValue(systemId,consumerId,brandId, actionScore);
                        decrements.put(scb, bav);
                    }
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }

        for(Map.Entry<SystemConsumerBrand, BrandAffinityValue> e : decrements.entrySet()){
            e.getValue().setDeltaScore(e.getValue().getDeltaScore() * -1);
            String keyStr = gson.toJson(e.getKey());
            String valStr = gson.toJson(e.getValue());
            logger.debug("Sending {} and {} to {}", keyStr, valStr, intermediateTopic);
            producerManager.getProducer().send(new ProducerRecord<>(intermediateTopic, keyStr, valStr));
        }
        updateLastDate(endDate);
    }

    final static String affinityLastDateKey = "AFFINITY_DECREMENT_FROM";
    public static String getLastDate() throws Exception {

        final String query = "SELECT value from config_parameters where key = '"+affinityLastDateKey+"'";
        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query)){

            if(rs.next()){
                return rs.getString(1);
            }else{
                throw new Exception("Parameter "+affinityLastDateKey+" not found");
            }
        }
    }

    public static void updateLastDate(String newDate) throws SQLException {
        final String query = "UPDATE config_parameters set value = '"+newDate+"' where key = '"+affinityLastDateKey+"'";
        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement() ){
            logger.info("Updating last date");
            st.executeUpdate(query);
            logger.info("Updated last date");
        }
    }

}
