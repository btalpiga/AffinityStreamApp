package com.nyble.consumerProcessors;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.nyble.facades.kafkaConsumer.RecordProcessor;
import com.nyble.main.AffinityActionsDict;
import com.nyble.main.App;
import com.nyble.streams.types.BrandAffinityValue;
import com.nyble.streams.types.SystemConsumerBrand;
import com.nyble.topics.Names;
import com.nyble.topics.TopicObjectsFactory;
import com.nyble.topics.consumerActions.ConsumerActionsValue;
import com.nyble.topics.consumerAttributes.ConsumerAttributesKey;
import com.nyble.topics.consumerAttributes.ConsumerAttributesValue;
import com.nyble.types.Subcampaign;
import com.nyble.util.DBUtil;
import com.nyble.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;


public class ActionProcessor implements RecordProcessor<String, String> {

    private final static Logger logger = LoggerFactory.getLogger(ActionProcessor.class);
    private final Gson localGson = new Gson();

    @Override
    public boolean process(ConsumerRecord<String, String> consumerRecord) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean processBatch(ConsumerRecords<String, String> consumerRecords) {
        Map<SystemConsumerBrand, BrandAffinityValue> increments = new HashMap<>();
        final String now = System.currentTimeMillis()+"";
        consumerRecords.forEach(action->{
            int externalSystemProvenience = action.topic().contains("rmc") ? Names.RMC_SYSTEM_ID : Names.RRP_SYSTEM_ID;
            ConsumerActionsValue actionValue = (ConsumerActionsValue) TopicObjectsFactory.fromJson(action.value(), ConsumerActionsValue.class);
            Pair<SystemConsumerBrand, BrandAffinityValue> actionScore;
            try {
                actionScore = getActionScoring(externalSystemProvenience, actionValue);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            if(actionScore != null){
                if(increments.containsKey(actionScore.leftSide)){
                    int dScore = increments.get(actionScore.leftSide).getDeltaScore();
                    increments.get(actionScore.leftSide).setDeltaScore(dScore + actionScore.rightSide.getDeltaScore());
                }else{
                    increments.put(actionScore.leftSide, actionScore.rightSide);
                }
            }
        });

        for(Map.Entry<SystemConsumerBrand, BrandAffinityValue> e : increments.entrySet()){
            SystemConsumerBrand k = e.getKey();
            BrandAffinityValue v = e.getValue();
            String brandScoreLabel = "affinity_"+v.getBrandId();
            String incrementScore = "+"+v.getDeltaScore();
            ConsumerAttributesKey cak = new ConsumerAttributesKey(k.getSystemId(), k.getConsumerId());
            ConsumerAttributesValue cav = new ConsumerAttributesValue(k.getSystemId()+"", k.getConsumerId()+"",
                    brandScoreLabel, incrementScore, now, now);
            String keyStr = cak.toJson();
            String valStr = cav.toJson();
            logger.debug("Sending {} and {} to {}", keyStr, valStr, Names.CONSUMER_ATTRIBUTES_TOPIC);
            App.producerManager.getProducer().send(new ProducerRecord<>(Names.CONSUMER_ATTRIBUTES_TOPIC, keyStr, valStr));
        }

        return true;
    }


    public Pair<SystemConsumerBrand, BrandAffinityValue> getActionScoring(int systemId,  ConsumerActionsValue cav) throws SQLException {

        if( (systemId == Names.RMC_SYSTEM_ID && Integer.parseInt(cav.getId()) <= App.lastRmcActionId) ||
                (systemId == Names.RRP_SYSTEM_ID && Integer.parseInt(cav.getId()) <= App.lastRrpActionId )){
            return null;
        }
        String subcampaignStr = cav.getPayloadJson().subcampaignId;
        if(subcampaignStr == null || subcampaignStr.isEmpty()){
            return null;
        }
        int subcampaignId = Integer.parseInt(subcampaignStr);
        Subcampaign subcampaign = getSubcampaign(systemId, subcampaignId);
        if(subcampaign == null){
            return null;
        }
        int brandId = subcampaign.getBrandId();
        if(brandId == 13 && subcampaign.getSystemId() == Names.RMC_SYSTEM_ID){
            brandId = 138;
        }else if(brandId == 486){
            if(cav.getPayloadJson().getValue() == null){
                return null;
            }else{
                String brandPrefix = getSkuBought(cav);
                if(brandPrefix.isEmpty()){
                    brandPrefix = getPrizeName(cav);
                }
                if(brandPrefix.startsWith("SB") || brandPrefix.startsWith("SO")){brandId = 125;}
                else if(brandPrefix.startsWith("WI")){brandId = 127;}
                else if(brandPrefix.startsWith("CA")){brandId = 117;}
                else if(brandPrefix.startsWith("LG")){brandId = 138;}
                else {return null;}
            }
        }else if(!AffinityActionsDict.allowedBrands.contains(brandId)){
            return null;
        }

        int actionScore = AffinityActionsDict.getScore(cav.getSystemId()+"",
                cav.getActionId()+"", cav.getPayloadJson());
        if(actionScore != 0){
            int consumerId = Integer.parseInt(cav.getConsumerId());
            SystemConsumerBrand scb = new SystemConsumerBrand(systemId, consumerId, brandId);
            BrandAffinityValue bav = new BrandAffinityValue(systemId, consumerId, brandId, actionScore);
            return new Pair<>(scb, bav);
        }else{
            return null;
        }
    }


    public String getSkuBought(ConsumerActionsValue cav){
        String rez = cav.getPayloadJson().getValue().sku_bought!= null ?
                cav.getPayloadJson().getValue().sku_bought.toUpperCase() : "";
        return !rez.isEmpty() && rez.length()>=2 ? rez.substring(0, 2) : "";
    }

    public String getPrizeName(ConsumerActionsValue cav){
        String rez = "";
        try{
            if(cav.getActionId().equals("1898")){
                JsonElement json = localGson.fromJson(cav.getPayloadJson().getRaw(), JsonElement.class);
                if(!json.isJsonNull() && json.isJsonObject()){
                    rez = json.getAsJsonObject().get("value").getAsJsonObject().get("prize_name").getAsString().toUpperCase();
                    if(rez.contains("SOBRANIE")){
                        rez = "SB";
                    }else if(rez.contains("CAMEL")){
                        rez = "CA";
                    }else if(rez.contains("WINSTON")){
                        rez = "WI";
                    }
                }
            }
        }catch(Exception e){
            logger.error(e.getMessage(), e);
        }
        return rez;
    }

    public Subcampaign getSubcampaign(int systemId, int subcampaignId) throws SQLException {
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
