import com.google.gson.Gson;
import com.nyble.topics.consumerActions.ConsumerActionsValue;
import junit.framework.TestCase;

public class ConsumerActionPayloadTest extends TestCase {

    public void test_jsonConvertor(){
        String json = "{\"touchpointId\" : 2, \"subcampaignId\" : 5361, \"userId\" : 0, " +
                "\"value\" : {\"new_value\": 35, \"old_value\": 14, \"sku_quantity\":10}, " +
                "\"posId\" : null, \"externalId\" : null, \"chanId\" : 5, \"prizeId\" : null, \"gainVal\" : null, " +
                "\"spendVal\" : null}";
        Gson gson = new Gson();

        ConsumerActionsValue.ConsumerActionsPayload cap = gson.fromJson(json, ConsumerActionsValue.ConsumerActionsPayload.class);
        assertNull(cap.getValue().getNewValueAsBool());

        assertEquals("10", cap.getValue().sku_quantity);
    }

    public void test_jsonConvertorNull(){
        String json = null;
        Gson gson = new Gson();

        ConsumerActionsValue.ConsumerActionsPayload cap = gson.fromJson(json, ConsumerActionsValue.ConsumerActionsPayload.class);
        assertNull(cap);
    }

    public void test_jsonConvertorEmpty(){
        String json = "{}";
        Gson gson = new Gson();

        ConsumerActionsValue.ConsumerActionsPayload cap = gson.fromJson(json, ConsumerActionsValue.ConsumerActionsPayload.class);
        assertNull(cap.getValue());
        assertNull(cap.subcampaignId);
    }
}
