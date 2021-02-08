package com.nyble.main;

import com.google.gson.Gson;
import com.nyble.consumerProcessors.ActionProcessor;
import com.nyble.exceptions.RuntimeSqlException;
import com.nyble.facades.kafkaConsumer.KafkaConsumerFacade;
import com.nyble.managers.ProducerManager;
import com.nyble.streams.types.BrandAffinityValue;
import com.nyble.streams.types.SystemConsumerBrand;
import com.nyble.topics.Names;
import com.nyble.topics.consumerActions.ConsumerActionsValue;
import com.nyble.topics.consumerAttributes.ConsumerAttributesKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesValue;
import com.nyble.types.Subcampaign;
import com.nyble.util.DBUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@SpringBootApplication(scanBasePackages = {"com.nyble.rest"})
public class App {

    public final static String KAFKA_CLUSTER_BOOTSTRAP_SERVERS = "10.100.1.17:9093";
    public final static Logger logger = LoggerFactory.getLogger(App.class);
    public static ProducerManager producerManager;
    final static Properties producerProps = new Properties();
    final static Properties consumerProps = new Properties();
    public static int lastRmcActionId;
    public static int lastRrpActionId;


    static{

        producerProps.put("bootstrap.servers", KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        producerProps.put("acks", "all");
        producerProps.put("retries", 5);
        producerProps.put("batch.size", 16384);
        producerProps.put("linger.ms", 1);
        producerProps.put("buffer.memory", 33554432);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerManager = ProducerManager.getInstance(producerProps);

        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000*60);

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            producerManager.getProducer().flush();
            producerManager.getProducer().close();
        }));
    }

    public static void initLastActionIds(){
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
    }

    public static List<KafkaConsumerFacade<String, String>> initKafkaConsumers(){
        KafkaConsumerFacade<String, String> consumerActionsFacade = new KafkaConsumerFacade<>(consumerProps,
                2, KafkaConsumerFacade.PROCESSING_TYPE_BATCH);
        consumerActionsFacade.subscribe(Arrays.asList(Names.CONSUMER_ACTIONS_RMC_TOPIC, Names.CONSUMER_ACTIONS_RRP_TOPIC));
        consumerActionsFacade.startPolling(Duration.ofSeconds(10), ActionProcessor.class);

        return Collections.singletonList(consumerActionsFacade);
    }



    public static void main(String[] args){

        try{
            ConfigurableApplicationContext restApp = SpringApplication.run(App.class, args);
            ExecutorService scheduler = scheduleBatchUpdate();

            initLastActionIds();
            List<KafkaConsumerFacade<String, String>> consumerFacades = initKafkaConsumers();

            Runtime.getRuntime().addShutdownHook(new Thread(()->{
                if(restApp != null){
                    logger.warn("Closing rest server");
                    restApp.close();
                    logger.warn("Rest server closed");
                }
                if(scheduler != null){
                    logger.warn("Closing decrement scheduler");
                    scheduler.shutdown();
                    try {
                        scheduler.awaitTermination(100, TimeUnit.SECONDS);
                        logger.warn("Decrement scheduler closed");
                    } catch (InterruptedException e) {
                        logger.error("Force killed scheduler", e);
                    }
                }
                for(KafkaConsumerFacade<String, String> consumerFacade : consumerFacades){
                    try {
                        logger.warn("Closing kafka consumer facade");
                        consumerFacade.stopPolling();
                        logger.warn("Kafka consumer facade closed");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }));
        }catch(Exception e){
            logger.error(e.getMessage(), e);
            logger.error("EXITING");
        }
    }

    public static ScheduledExecutorService scheduleBatchUpdate(){
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final Runnable task = ()->{
            try {
                logger.info("Start update ");
                updateScoresEveryHour();
                logger.info("End update");
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        };
        Duration delay = Duration.between(Instant.now(), Instant.now().plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS));
        scheduler.scheduleAtFixedRate(task, delay.toMillis(), Duration.ofHours(1).toMillis(), TimeUnit.MILLISECONDS);
        logger.info("Task will start in: "+delay.toMillis()+" millis");

        return scheduler;
    }


    private static void updateScoresEveryHour() throws Exception {
        Gson gson = new Gson();
        Map<SystemConsumerBrand, BrandAffinityValue> decrements = new HashMap<>();
        final String startDate = getLastDate();
        Calendar now = new GregorianCalendar();
        now.add(Calendar.YEAR, -2);
        now.set(Calendar.MILLISECOND, 0);
        now.set(Calendar.SECOND, 0);
        now.set(Calendar.MINUTE, 0);
        final String endDate = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(now.getTime());
        final String currentTime = System.currentTimeMillis()+"";

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
            SystemConsumerBrand k = e.getKey();
            BrandAffinityValue v = e.getValue();
            String brandScoreLabel = "affinity_"+v.getBrandId();
            String incrementScore = "-"+v.getDeltaScore();
            ConsumerAttributesKey cak = new ConsumerAttributesKey(k.getSystemId(), k.getConsumerId());
            ConsumerAttributesValue cav = new ConsumerAttributesValue(k.getSystemId()+"", k.getConsumerId()+"",
                    brandScoreLabel, incrementScore, currentTime, currentTime);
            String keyStr = cak.toJson();
            String valStr = cav.toJson();
            logger.debug("Sending {} and {} to {}", keyStr, valStr, Names.CONSUMER_ATTRIBUTES_TOPIC);
            producerManager.getProducer().send(new ProducerRecord<>(Names.CONSUMER_ATTRIBUTES_TOPIC, keyStr, valStr));
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

    public static Subcampaign getSubcampaign(int systemId, int subcampaignId) throws SQLException {
        final String queryLocal = String.format("select system_id, id, campaign_id, brand_id, date_create " +
                "from ref.subcampaignes " +
                "where system_id = %d and id = %d", systemId, subcampaignId);
        final String queryExternal = String.format("select system_id, id, campaign_id, brand_id, date_create " +
                "from ref.load_external_subcampaign " +
                "where system_id = %d and id = %d", systemId, subcampaignId);
        final String queryInsertExternal = "INSERT INTO ref.subcampaignes (system_id, id, campaign_id, brand_id, date_create) " +
                "values (?,?,?,?,?) " +
                "on conflict on constraint unique_external_subcampaign do nothing";
        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(queryLocal)){
            if(rs.next()){
                int brandId = rs.getInt("brand_id");
                return new Subcampaign(systemId, subcampaignId, brandId);
            }else{
                try(Statement externalSt = conn.createStatement();
                    PreparedStatement insertExternal = conn.prepareStatement(queryInsertExternal);
                    ResultSet externalRs = externalSt.executeQuery(queryExternal)){

                    if(externalRs.next()){
                        int campaignId = externalRs.getInt("campaign_id");
                        int brandId = externalRs.getInt("brand_id");
                        Timestamp dateCreate = externalRs.getTimestamp("date_create");

                        insertExternal.setInt(1, systemId);
                        insertExternal.setInt(2, subcampaignId);
                        insertExternal.setInt(3, campaignId);
                        insertExternal.setInt(4, brandId);
                        insertExternal.setTimestamp(5, dateCreate);
                        insertExternal.executeUpdate();

                        return new Subcampaign(systemId, subcampaignId, brandId);
                    }else{
                        return null;
                    }
                }
            }
        }
    }
}
