import com.nyble.main.AffinityActionsDict;
import com.nyble.streams.types.SubcampaignesKey;
import com.nyble.topics.TopicObjectsFactory;
import com.nyble.topics.consumerActions.ConsumerActionsValue;
import junit.framework.TestCase;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AffinitySourceCreatorTests extends TestCase {

    public void test_1(){
        String json  = "{\"id\":677515389,\"systemId\":1,\"consumerId\":12107037,\"actionId\":1814,\"payloadJson\":\"" +
                "{\\\"" +
                    "posId\\\": null, \\\"value\\\": " +
                        "{\\\"sku\\\": \\\"SBSSWH\\\", \\\"code\\\": \\\"PK56CHF5N\\\", \\\"canal\\\": \\\"web\\\", \\\"platform\\\": \\\"web\\\", \\\"code_type\\\": 1}," +
                    "\\\"chanId\\\": \\\"5\\\", \\\"userId\\\": \\\"0\\\", \\\"prizeId\\\": null, \\\"valGain\\\": null, \\\"valSpend\\\": null, \\\"externalId\\\": null, " +
                    "\\\"touchpointId\\\": \\\"2\\\", \\\"subcampaignId\\\": \\\"5376\\\"" +
                "}\"," +
                "\"externalSystemDate\":\"1606505486000\",\"localSystemDate\":\"1606505513382\"}";

//        String json = "{\"id\":677274466,\"systemId\":1,\"consumerId\":12107037,\"actionId\":1814,\"payloadJson\":\"" +
//                "{\\\"" +
//                    "posId\\\": null, \\\"value\\\": " +
//                        "{\\\"sku\\\": \\\"SBSSWH\\\", \\\"code\\\": \\\"A51A95ARH\\\", \\\"canal\\\": \\\"web\\\", \\\"platform\\\": \\\"web\\\", \\\"code_type\\\": 1}, " +
//                    "\\\"chanId\\\": \\\"5\\\", \\\"userId\\\": \\\"0\\\", \\\"prizeId\\\": null, \\\"valGain\\\": null, \\\"valSpend\\\": null, \\\"externalId\\\": null, " +
//                    "\\\"touchpointId\\\": \\\"2\\\", \\\"subcampaignId\\\": \\\"5376\\\"" +
//                "}\"," +
//                "\"externalSystemDate\":\"1606505486000\",\"localSystemDate\":\"1606505513382\"}";

        int lastAction = 672820891;
        ConsumerActionsValue cav = (ConsumerActionsValue) TopicObjectsFactory.fromJson(json, ConsumerActionsValue.class);
        if(Integer.parseInt(cav.getId()) > lastAction && AffinityActionsDict.filter(cav)){
            ConsumerActionsValue.ConsumerActionsPayload consumerActionPayload = cav.getPayloadJson();
            if(consumerActionPayload!= null && consumerActionPayload.subcampaignId != null){
                int subcampaignId = Integer.parseInt(consumerActionPayload.subcampaignId);
                int systemId = Integer.parseInt(cav.getSystemId());
//                String skAsString = gson.toJson(new SubcampaignesKey(systemId, subcampaignId));
//                producerManager.getProducer().send(new ProducerRecord<>(sourceTopic, skAsString, record.value()));
//                logger.debug("Create {} topic input",sourceTopic);
            }else{
                fail();
            }
        }else{
            fail();
        }
    }
}
