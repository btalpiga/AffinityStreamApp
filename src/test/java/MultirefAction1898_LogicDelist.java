import com.nyble.consumerProcessors.ActionProcessor;
import com.nyble.streams.types.BrandAffinityValue;
import com.nyble.streams.types.SystemConsumerBrand;
import com.nyble.topics.TopicObjectsFactory;
import com.nyble.topics.consumerActions.ConsumerActionsValue;
import com.nyble.types.Subcampaign;
import com.nyble.util.Pair;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MultirefAction1898_LogicDelist extends TestCase {


    @Spy
    ActionProcessor apSpy;

    @Test
    public void testActionNotMultiref() throws SQLException {

        final int systemId = 1;
        final int consumerId = 1;
        final int brandId = 125;
        final int subcampaignId = 1;
        doReturn(new Subcampaign(systemId, subcampaignId, brandId)).when(apSpy).getSubcampaign(anyInt(), anyInt());


        String actionJson = "{\n" +
                "  \"id\": 736539415,\n" +
                "  \"systemId\": "+systemId+",\n" +
                "  \"consumerId\": "+consumerId+",\n" +
                "  \"actionId\": 1898,\n" +
                "  \"payloadJson\": \"{\\\"posId\\\": null, \\\"value\\\": {\\\"code\\\": \\\"7ETTLJ7A\\\", \\\"prize_id\\\": \\\"179\\\", \\\"claim_date\\\": \\\"2021-02-27 21:39:06\\\", \\\"prize_name\\\": \\\"Bricheta Bic Winston\\\", \\\"campaign_id\\\": \\\"10282\\\", \\\"consumer_id\\\": \\\"7308968\\\"}, \\\"chanId\\\": null, \\\"userId\\\": \\\"0\\\", \\\"prizeId\\\": null, \\\"valGain\\\": null, \\\"valSpend\\\": null, \\\"externalId\\\": \\\"0\\\", \\\"touchpointId\\\": \\\"8\\\", \\\"subcampaignId\\\": \\\""+subcampaignId+"\\\"}\",\n" +
                "  \"externalSystemDate\": \"1614454746000\",\n" +
                "  \"localSystemDate\": \"1614455470182\"\n" +
                "}";
        ConsumerActionsValue cav = (ConsumerActionsValue) TopicObjectsFactory.fromJson(actionJson, ConsumerActionsValue.class);
        Pair<SystemConsumerBrand, BrandAffinityValue> score =  apSpy.getActionScoring(systemId, cav);
        assertNotNull(score);

        SystemConsumerBrand scb = score.leftSide;
        BrandAffinityValue bav = score.rightSide;

        assertNotNull(scb);
        assertEquals(systemId, scb.getSystemId());
        assertEquals(consumerId, scb.getConsumerId());
        assertEquals(brandId, scb.getBrandId());

        assertNotNull(bav);
        assertEquals(systemId, bav.getSystemId());
        assertEquals(consumerId, bav.getConsumerId());
        assertEquals(brandId, bav.getBrandId());
        assertEquals(10, bav.getDeltaScore());

        verify(apSpy, Mockito.times(1)).getSubcampaign(anyInt(),anyInt());
        verify(apSpy, never()).getPrizeName(any());
        verify(apSpy, never()).getSkuBought(any());
    }

    @Test
    public void testActionMultiref() throws SQLException {

        final int systemId = 1;
        final int consumerId = 1;
        final int brandId = 486;
        final int subcampaignId = 1;
        final int actualBrandId = 127;
        doReturn(new Subcampaign(systemId, subcampaignId, brandId)).when(apSpy).getSubcampaign(anyInt(), anyInt());


        String actionJson = "{\n" +
                "  \"id\": 736539415,\n" +
                "  \"systemId\": "+systemId+",\n" +
                "  \"consumerId\": "+consumerId+",\n" +
                "  \"actionId\": 1898,\n" +
                "  \"payloadJson\": \"{\\\"posId\\\": null, \\\"value\\\": {\\\"code\\\": \\\"7ETTLJ7A\\\", \\\"prize_id\\\": \\\"179\\\", \\\"claim_date\\\": \\\"2021-02-27 21:39:06\\\", \\\"prize_name\\\": \\\"Cadou pachet Winston\\\", \\\"campaign_id\\\": \\\"10282\\\", \\\"consumer_id\\\": \\\"7308968\\\"}, \\\"chanId\\\": null, \\\"userId\\\": \\\"0\\\", \\\"prizeId\\\": null, \\\"valGain\\\": null, \\\"valSpend\\\": null, \\\"externalId\\\": \\\"0\\\", \\\"touchpointId\\\": \\\"8\\\", \\\"subcampaignId\\\": \\\""+subcampaignId+"\\\"}\",\n" +
                "  \"externalSystemDate\": \"1614454746000\",\n" +
                "  \"localSystemDate\": \"1614455470182\"\n" +
                "}";
        ConsumerActionsValue cav = (ConsumerActionsValue) TopicObjectsFactory.fromJson(actionJson, ConsumerActionsValue.class);
        Pair<SystemConsumerBrand, BrandAffinityValue> score =  apSpy.getActionScoring(systemId, cav);
        assertNotNull(score);

        SystemConsumerBrand scb = score.leftSide;
        BrandAffinityValue bav = score.rightSide;

        assertNotNull(scb);
        assertEquals(systemId, scb.getSystemId());
        assertEquals(consumerId, scb.getConsumerId());
        assertEquals(actualBrandId, scb.getBrandId());

        assertNotNull(bav);
        assertEquals(systemId, bav.getSystemId());
        assertEquals(consumerId, bav.getConsumerId());
        assertEquals(actualBrandId, bav.getBrandId());
        assertEquals(10, bav.getDeltaScore());

        verify(apSpy, times(1)).getSubcampaign(anyInt(),anyInt());
        verify(apSpy, times(1)).getPrizeName(any());
        verify(apSpy, times(1)).getSkuBought(any());
    }
}
