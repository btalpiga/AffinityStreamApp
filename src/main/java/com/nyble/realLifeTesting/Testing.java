package com.nyble.realLifeTesting;

import com.google.gson.Gson;
import com.nyble.main.AffinityActionsDict;
import com.nyble.streams.types.BrandAffinityValue;
import com.nyble.streams.types.SystemConsumerBrand;
import com.nyble.topics.consumerActions.ConsumerActionsValue;
import com.nyble.util.DBUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Testing {

    public static void main(String[] args) throws ParseException {

        Gson gson = new Gson();
        Map<SystemConsumerBrand, BrandAffinityValue> decrements = new HashMap<>();

        final String startDate = "2019-01-01";
        Calendar now = new GregorianCalendar();
        now.setTime(new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-15"));
        now.add(Calendar.YEAR, -2);
        now.set(Calendar.MILLISECOND, 0);
        now.set(Calendar.SECOND, 0);
        now.set(Calendar.MINUTE, 0);
        final String endDate = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(now.getTime());

        final String query = "select ca.system_id, ca.consumer_id, r.brand_id , ca.action_id, ca.payload_json->>'value' as value, \n" +
                "(ca.payload_json ->>'subcampaignId')::int, ca.payload_json::text as payload\n" +
                "from consumer_actions ca \n" +
                "join recoded_subcampaignes r on (ca.payload_json ->>'subcampaignId')::int = r.id and ca.system_id = r.system_id\n" +
                "where ca.external_system_date > '"+startDate+"' and ca.external_system_date <= '"+endDate+"' and \n" +
                "r.brand_id in (13,"+ AffinityActionsDict.allowedBrands.stream().map(bId->bId+"").collect(Collectors.joining(","))+") " +
                "and ca.system_id = 1 and  consumer_id = 11279641";

//        final String query = "select ca.system_id, ca.consumer_id, s.brand_id , ca.action_id, ca.payload_json->>'value' as value, \n" +
//                "(ca.payload_json ->>'subcampaignId')::int, ca.payload_json::text as payload, ca.external_system_date \n" +
//                "from consumer_actions ca \n" +
//                "join recoded_subcampaignes s on (payload_json ->> 'subcampaignId')::int = s.id and s.system_id = 1\n" +
//                "join affinity.action_scores sc on sc.system_id = 1 and sc.action_id  = ca.action_id \n" +
//                "where ca.system_id = 1 and  consumer_id = 11279641 \n" +
//                "and ca.external_system_date <= '2021-01-15'::date - '2 year'::interval\n" +
//                "order by external_system_date";
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
            e.printStackTrace();
        }

        decrements.forEach((key, value) -> System.out.println(key.getBrandId() + ": " + value.getDeltaScore()));

    }
}
