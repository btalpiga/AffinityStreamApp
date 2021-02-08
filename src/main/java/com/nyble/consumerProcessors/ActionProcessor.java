package com.nyble.consumerProcessors;

import com.google.gson.Gson;
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
import com.nyble.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class ActionProcessor implements RecordProcessor<String, String> {

    private final static Logger logger = LoggerFactory.getLogger(ActionProcessor.class);

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
        Subcampaign subcampaign = App.getSubcampaign(systemId, subcampaignId);
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
                String skuBought = cav.getPayloadJson().getValue().sku_bought!= null ?
                        cav.getPayloadJson().getValue().sku_bought : "";
                if(skuBought.toUpperCase().startsWith("SB") || skuBought.toUpperCase().startsWith("SO")){brandId = 125;}
                else if(skuBought.toUpperCase().startsWith("WI")){brandId = 127;}
                else if(skuBought.toUpperCase().startsWith("CA")){brandId = 117;}
                else if(skuBought.toUpperCase().startsWith("LG")){brandId = 138;}
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
}
