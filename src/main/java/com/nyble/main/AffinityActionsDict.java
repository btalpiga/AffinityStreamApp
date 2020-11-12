package com.nyble.main;

import com.google.gson.Gson;
import com.nyble.topics.consumerActions.ConsumerActionsValue;
import com.nyble.types.ConsumerActionDescriptor;
import com.nyble.util.DBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class AffinityActionsDict {
    static Map<String, ConsumerActionDescriptor> affinityActions = new HashMap<>();
    static final Gson jsonConverter = new Gson();
    static Map<String, Integer> productScoreMap;
    //117 = CAMEL   125 = SOBRANIE   127 = WINSTON   138 = LOGIC   486 = Multiref.brand
    final public static Set<Integer> allowedBrands = new HashSet<>(Arrays.asList(117,125,127,138,486));
    final static Logger logger = LoggerFactory.getLogger(AffinityActionsDict.class);
    static {

        affinityActions.put("1#1964", new ConsumerActionDescriptor(1964,"121 offline interaction","1",
                true,
                (cap)->{
                    //2 per actiune + 10 X "sku_quantity"
                    if(cap == null){
                        return 2;
                    }
                    int init = 2;
                    ConsumerActionsValue.ConsumerActionsPayload.Value valueS = cap.getValue();
                    if(valueS != null){
                        Integer skuQ = (valueS.sku_quantity == null ? 0 : Integer.parseInt(valueS.sku_quantity));
                        init = init + 10 * skuQ;
                    }
                    return init;
                }));

        affinityActions.put("1#1740", new ConsumerActionDescriptor(1740,"121 Interaction","1",
                true,
                (cap)->{
                    //2 per actiune + 10 X "sku_quantity"
                    if(cap == null){
                        return 2;
                    }
                    int init = 2;
                    ConsumerActionsValue.ConsumerActionsPayload.Value valueS = cap.getValue();
                    if(valueS != null){
                        Integer skuQ = (valueS.sku_quantity == null ? 0 : Integer.parseInt(valueS.sku_quantity));
                        init = init + 10 * skuQ;
                    }
                    return init;
                }));
        affinityActions.put("1#2080", new ConsumerActionDescriptor(2080,"Change opt in","1",
                true,
                (cap)->{
                    //1, doar daca { "new_value": true, "old_value": false }
                    if(cap == null){
                        return 0;
                    }
                    ConsumerActionsValue.ConsumerActionsPayload.Value valueS = cap.getValue();
                    if(valueS != null){
                        Boolean newValue = valueS.getNewValueAsBool();
                        Boolean oldValue = valueS.getOldValueAsBool();
                        if(newValue == null || oldValue == null){
                            return 0;
                        }
                        return newValue && !oldValue ? 1 : 0;
                    }else{
                        return 0;
                    }
                }));

        //----------
        affinityActions.put("1#1814", new ConsumerActionDescriptor(1814,"Entered correct code",10,"1"));
        affinityActions.put("1#1898", new ConsumerActionDescriptor(1898,"Prize redeemed WinPrize.ro",10,"1"));
        affinityActions.put("1#3083", new ConsumerActionDescriptor(3083,"Referral gift code sent",9,"1"));
        affinityActions.put("1#3147", new ConsumerActionDescriptor(3147,"Referral benefit",9,"1"));
        affinityActions.put("1#1866", new ConsumerActionDescriptor(1866,"Completed",8,"1"));
        affinityActions.put("1#3081", new ConsumerActionDescriptor(3081,"Referral link used by friend for web account",8,"1"));
        affinityActions.put("1#3079", new ConsumerActionDescriptor(3079,"generare link",7,"1"));
        affinityActions.put("1#1728", new ConsumerActionDescriptor(1728,"Initiated",7,"1"));
        affinityActions.put("1#2051", new ConsumerActionDescriptor(2051,"Engagement Alegere",6,"1"));
        affinityActions.put("1#1755", new ConsumerActionDescriptor(1755,"Visited web page",6,"1"));
        affinityActions.put("1#1828", new ConsumerActionDescriptor(1828,"Login website",6,"1"));
        affinityActions.put("1#1858", new ConsumerActionDescriptor(1858,"Web account created",6,"1"));
        affinityActions.put("1#3017", new ConsumerActionDescriptor(3017,"Validated as winner_with_CI_x000D_",5,"1"));
        affinityActions.put("1#2057", new ConsumerActionDescriptor(2057,"Data Validationw.o_CI_Validated_Opt_In",5,"1"));
        affinityActions.put("1#3037", new ConsumerActionDescriptor(3037,"Research_Completed_x000D_",5,"1"));
        affinityActions.put("1#1763", new ConsumerActionDescriptor(1763,"SMS User",4,"1"));
        affinityActions.put("1#1832", new ConsumerActionDescriptor(1832,"Click_link Email",4,"1"));
        affinityActions.put("1#3149", new ConsumerActionDescriptor(3149,"click_short_url_with_data",4,"1"));
        affinityActions.put("1#3016", new ConsumerActionDescriptor(3016,"Validated as winner_w.o_CI_x000D_",3,"1"));
        affinityActions.put("1#3090", new ConsumerActionDescriptor(3090,"Premiu_Sobranie_Gold",3,"1"));
        affinityActions.put("1#3089", new ConsumerActionDescriptor(3089,"Premiu_Sobranie_Black",3,"1"));
        affinityActions.put("1#3092", new ConsumerActionDescriptor(3092,"Validat_multumit",3,"1"));
        affinityActions.put("1#1833", new ConsumerActionDescriptor(1833,"Fraud (apel de la consumator cu subiect Frauda)",3,"1"));
        affinityActions.put("1#1851", new ConsumerActionDescriptor(1851,"Open Email",3,"1"));
        affinityActions.put("1#2048", new ConsumerActionDescriptor(2048,"Edit profile",3,"1"));
        affinityActions.put("1#1784", new ConsumerActionDescriptor(1784,"Change account password",3,"1"));
        affinityActions.put("1#3135", new ConsumerActionDescriptor(3135,"change_password",3,"1"));
        affinityActions.put("1#2098", new ConsumerActionDescriptor(2098,"Marketo email validated",2,"1"));
        affinityActions.put("1#3064", new ConsumerActionDescriptor(3064,"Logic 121 Confirmed Interaction_x000D_",2,"1"));
        affinityActions.put("1#3065", new ConsumerActionDescriptor(3065,"Logic 121 Offline Interaction_x000D_",2,"1"));
        affinityActions.put("1#1730", new ConsumerActionDescriptor(1730,"Email address confirmed",2,"1"));
        affinityActions.put("1#2067", new ConsumerActionDescriptor(2067,"confirm_marketing_agreement",2,"1"));



        affinityActions.put("2#2080", new ConsumerActionDescriptor(2080, "Change opt in",  "2",
                true,
                (cap)->{
                    //1, doar daca { "new_value": true, "old_value": false }
                    if(cap == null){
                        return 0;
                    }
                    ConsumerActionsValue.ConsumerActionsPayload.Value valueS = cap.getValue();
                    if(valueS != null){
                        Boolean newValue = valueS.getNewValueAsBool();
                        Boolean oldValue = valueS.getOldValueAsBool();
                        if(newValue == null || oldValue == null){
                            return 0;
                        }
                        return newValue && !oldValue ? 1 : 0;
                    }else{
                        return 0;
                    }
                }));
        affinityActions.put("2#3064", new ConsumerActionDescriptor(3064, "crm_consumer_action_new_order", "2",
                true,
                (cap)->{
                    //1 capsula = 10 pct, 1 device standard = 10 pct, 1 device cristale = 20 pct, 1 accesoriu = 12 pct
                    if(cap == null){
                        return 0;
                    }
                    int score = 0;
                    try{
                        Map<String, Object> payload = jsonConverter.fromJson(cap.getRaw(), Map.class);
                        if(payload == null){
                            return 0;
                        }
                        Map<String, Object> command = (Map<String, Object>) payload.get("value");
                        if(command == null){
                            return 0;
                        }
                        List<Map<String, Object>> products = (List<Map<String, Object>>) ((Map)(command.get("Products"))).get("Product");
                        for(Map<String, Object> product : products){
                            String productWebSku = product.get("ProductSKU")+"";
                            int productScore = AffinityActionsDict.getSkuScore(productWebSku);
                            score += Integer.parseInt(product.get("Quantity")+"")*productScore;
                        }
                        return score;
                    }catch(Exception e){
                        logger.error(e.getMessage(), e);
                        return score;
                    }
                }));

        affinityActions.put("2#1898", new ConsumerActionDescriptor(1898, "Prize redeemed WinPrize.ro",  "2",
                true,
                (cap)->{
                    //1 capsula = 10 pct, 1 device standard = 10 pct, 1 device cristale = 20 pct, 1 accesoriu = 12 pct
                    if(cap == null){ return 0;}
                    String jsonStr = cap.getRaw();
                    if(jsonStr == null){return 0;}

                    final String prizeNameTag = "prize_name";
                    String prizeName = extractTagValue(jsonStr, prizeNameTag);
                    if(prizeName.toUpperCase().contains("DEVICE") && prizeName.toUpperCase().contains("CRYSTALE")){
                        return 20;
                    }else if(prizeName.toUpperCase().contains("DEVICE") || prizeName.toUpperCase().contains("CAPSULES")){
                        return 10;
                    }else if (!prizeName.isEmpty()){
                        return 12;
                    }else {
                        return 0;
                    }

                }));

        //----------------------
        affinityActions.put("2#1775", new ConsumerActionDescriptor(1775, "Entered correct stamp code", 10, "2"));
        affinityActions.put("2#3074", new ConsumerActionDescriptor(3074, "crm_logic_comanda_delivered", 10, "2"));
        affinityActions.put("2#3069", new ConsumerActionDescriptor(3069, "new_magento_consumer", 6, "2"));
        affinityActions.put("2#1832", new ConsumerActionDescriptor(1832, "Click_link Email", 4, "2"));
        affinityActions.put("2#1894", new ConsumerActionDescriptor(1894, "Prize won (Referral)", 4, "2"));
        affinityActions.put("2#1851", new ConsumerActionDescriptor(1851, "Open Email", 3, "2"));
        affinityActions.put("2#3068", new ConsumerActionDescriptor(3068, "change_consumer_magento", 3, "2"));
        affinityActions.put("2#3071", new ConsumerActionDescriptor(3071, "Reclamatii_Schimbare device_Logic_Compact_Slate_Grey", 3, "2"));
        affinityActions.put("2#3105", new ConsumerActionDescriptor(3105, "Schimbare device_Logic Compact Steel Blue", 3, "2"));
        affinityActions.put("2#3103", new ConsumerActionDescriptor(3103, "Prize Management Platform_Reclamatii_Schimbare Capsula cu nicotina Tobacco 18mg___", 3, "2"));
        affinityActions.put("2#3106", new ConsumerActionDescriptor(3106, "Schimbare device_Logic Compact Rose Gold", 3, "2"));
        affinityActions.put("2#3107", new ConsumerActionDescriptor(3107, "Schimbare Capsula cu nicotina Berry Mint 12mg", 3, "2"));
        affinityActions.put("2#3108", new ConsumerActionDescriptor(3108, "Schimbare Capsula cu nicotina Cherry 12mg", 3, "2"));
        affinityActions.put("2#3109", new ConsumerActionDescriptor(3109, "Schimbare Capsula cu nicotina Menthol 18mg", 3, "2"));
        affinityActions.put("2#3110", new ConsumerActionDescriptor(3110, "Schimbare device Logic Compact Champagne Gold", 3, "2"));
        affinityActions.put("2#3111", new ConsumerActionDescriptor(3111, "Schimbare device Logic Compact Emerald Green", 3, "2"));
        affinityActions.put("2#3112", new ConsumerActionDescriptor(3112, "Schimbare device LOGIC COMPACT Scarlet Red", 3, "2"));
        affinityActions.put("2#3113", new ConsumerActionDescriptor(3113, "Update_Consumer_Infoline_Inbound_Validation", 3, "2"));
        affinityActions.put("2#3358", new ConsumerActionDescriptor(3358, "Produse_Reclamatii_Device_Slate Grey_Bateria si capsula nu se pot conecta cu succes_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3359", new ConsumerActionDescriptor(3359, "Produse_Reclamatii_Device_Rose Gold_Bateria si capsula nu se pot conecta cu succes_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3360", new ConsumerActionDescriptor(3360, "Produse_Reclamatii_Device_Steel Blue_Bateria si capsula nu se pot conecta cu succes_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3349", new ConsumerActionDescriptor(3349, "Schimbare Capsula cu nicotina Toasted Tobacco 12mg", 3, "2"));
        affinityActions.put("2#3350", new ConsumerActionDescriptor(3350, "Schimbare Caspula cu nicotina Strawberry 12mg", 3, "2"));
        affinityActions.put("2#3351", new ConsumerActionDescriptor(3351, "Schimbare Capsula cu nicotina Nutty Cream 12mg", 3, "2"));
        affinityActions.put("2#3352", new ConsumerActionDescriptor(3352, "Schimbare incarcator", 3, "2"));
        affinityActions.put("2#3353", new ConsumerActionDescriptor(3353, "Schimbare capsula cu Nicotina Intense Tobacco 18Mg", 3, "2"));
        affinityActions.put("2#3354", new ConsumerActionDescriptor(3354, "Schimbare Capsula Cu Nicotina Intense Menthol 18Mg", 3, "2"));
        affinityActions.put("2#3355", new ConsumerActionDescriptor(3355, "Schimbare Capsula Cu Nicotina Intense Mixed Berries", 3, "2"));
        affinityActions.put("2#3356", new ConsumerActionDescriptor(3356, "Schimbare Capsula Cu Nicotina Intense Caramel Banana 18Mg", 3, "2"));
        affinityActions.put("2#3357", new ConsumerActionDescriptor(3357, "Schimbare Capsula Cu Nicotina Intense Chai 18Mg", 3, "2"));
        affinityActions.put("2#3364", new ConsumerActionDescriptor(3364, "Produse_Reclamatii_Device_Slate Grey_Device-ul a functionat insa acum nu mai functioneaza_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3365", new ConsumerActionDescriptor(3365, "Produse_Reclamatii_Device_Rose Gold_Device-ul a functionat insa acum nu mai functioneaza_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3366", new ConsumerActionDescriptor(3366, "Produse_Reclamatii_Device_Steel Blue_Device-ul a functionat insa acum nu mai functioneaza_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3367", new ConsumerActionDescriptor(3367, "Produse_Reclamatii_Device_Emerald Green_Device-ul a functionat insa acum nu mai functioneaza_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3368", new ConsumerActionDescriptor(3368, "Produse_Reclamatii_Device_Champagne Gold_Device-ul a functionat insa acum nu mai functioneaza_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3370", new ConsumerActionDescriptor(3370, "Produse_Reclamatii_Device_Slate Grey_Alte probleme alte device-ului_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3371", new ConsumerActionDescriptor(3371, "Produse_Reclamatii_Device_Rose Gold_Alte probleme alte device-ului_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3372", new ConsumerActionDescriptor(3372, "Produse_Reclamatii_Device_Steel Blue_Alte probleme alte device-ului_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3378", new ConsumerActionDescriptor(3378, "Produse_Reclamatii_Device_Steel Blue_Device-ul nu se aprinde la apasarea butonului de pornire_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3382", new ConsumerActionDescriptor(3382, "Produse_Reclamatii_Device_Slate Grey_Device-ul se supraincalzeste si emana caldura excesiva_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3383", new ConsumerActionDescriptor(3383, "Produse_Reclamatii_Device_Rose Gold_Device-ul se supraincalzeste si emana caldura excesiva_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3384", new ConsumerActionDescriptor(3384, "Produse_Reclamatii_Device_Steel Blue_Device-ul se supraincalzeste si emana caldura excesiva_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3388", new ConsumerActionDescriptor(3388, "Produse_Reclamatii_Device_Slate Grey_Bateria se descarca intr-un timp foarte scurt_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3389", new ConsumerActionDescriptor(3389, "Produse_Reclamatii_Device_Rose Gold_Bateria se descarca intr-un timp foarte scurt_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3390", new ConsumerActionDescriptor(3390, "Produse_Reclamatii_Device_Steel Blue_Bateria se descarca intr-un timp foarte scurt_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3394", new ConsumerActionDescriptor(3394, "Produse_Reclamatii_Device_Slate Grey_Device-ul nu se incarca odata ce acesta este conectat la incarcator_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3395", new ConsumerActionDescriptor(3395, "Produse_Reclamatii_Device_Rose Gold_Device-ul nu se incarca odata ce acesta este conectat la incarcator_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3396", new ConsumerActionDescriptor(3396, "Produse_Reclamatii_Device_Steel Blue_Device-ul nu se incarca odata ce acesta este conectat la incarcator_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3397", new ConsumerActionDescriptor(3397, "Produse_Reclamatii_Device_Emerald Green_Device-ul nu se incarca odata ce acesta este conectat la incarcator_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3400", new ConsumerActionDescriptor(3400, "Produse_Reclamatii_Device_Slate Grey_Bateria nu functioneaza_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3401", new ConsumerActionDescriptor(3401, "Produse_Reclamatii_Device_Rose Gold_Bateria nu functioneaza_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3402", new ConsumerActionDescriptor(3402, "Produse_Reclamatii_Device_Steel Blue_Bateria nu functioneaza_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3419", new ConsumerActionDescriptor(3419, "Produse_Reclamatii_Device_Rose Gold_Alte probleme alte bateriei_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3420", new ConsumerActionDescriptor(3420, "Produse_Reclamatii_Device_Steel Blue_Alte probleme alte bateriei_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3424", new ConsumerActionDescriptor(3424, "Produse_Reclamatii_Device_Slate Grey_Indicatorul LED functioneaza intr-un mod neobisnuit_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3425", new ConsumerActionDescriptor(3425, "Produse_Reclamatii_Device_Rose Gold_Indicatorul LED functioneaza intr-un mod neobisnuit_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3426", new ConsumerActionDescriptor(3426, "Produse_Reclamatii_Device_Steel Blue_Indicatorul LED functioneaza intr-un mod neobisnuit_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3430", new ConsumerActionDescriptor(3430, "Produse_Reclamatii_Device_Slate Grey_Bateria se supraincalzeste si emana caldura excesiva - fara arsuri provocate_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3431", new ConsumerActionDescriptor(3431, "Produse_Reclamatii_Device_Rose Gold_Bateria se supraincalzeste si emana caldura excesiva - fara arsuri provocate_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3432", new ConsumerActionDescriptor(3432, "Produse_Reclamatii_Device_Steel Blue_Bateria se supraincalzeste si emana caldura excesiva - fara arsuri provocate_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3437", new ConsumerActionDescriptor(3437, "Produse_Reclamatii_Incarcator_USB Charger_Incarcatorul nu incarca bateria_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3445", new ConsumerActionDescriptor(3445, "Produse_Reclamatii_Capsula_Menthol 18mg_Curge lichid din capsula_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3446", new ConsumerActionDescriptor(3446, "Produse_Reclamatii_Capsula_Toasted Tobacco 12mg_Curge lichid din capsula_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3582", new ConsumerActionDescriptor(3582, "Produse_Reclamatii_Capsula_Tobacco 18mg_Inlocuire capsula martor_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3576", new ConsumerActionDescriptor(3576, "Produse_Reclamatii_Capsula_Berry Mint 12mg_Inlocuire capsula martor_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3577", new ConsumerActionDescriptor(3577, "Produse_Reclamatii_Capsula_Menthol 18mg_Inlocuire capsula martor_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3578", new ConsumerActionDescriptor(3578, "Produse_Reclamatii_Capsula_Toasted Tobacco 12mg_Inlocuire capsula martor_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3579", new ConsumerActionDescriptor(3579, "Produse_Reclamatii_Capsula_Strawberry 12mg_Inlocuire capsula martor_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3581", new ConsumerActionDescriptor(3581, "Produse_Reclamatii_Capsula_Cherry 12mg_Inlocuire capsula martor_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3583", new ConsumerActionDescriptor(3583, "Produse_Reclamatii_Capsula_Intense Caramel Banana 18Mg _Inlocuire capsula martor_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3584", new ConsumerActionDescriptor(3584, "Produse_Reclamatii_Capsula_Intense Menthol 18mg_Inlocuire capsula martor_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3585", new ConsumerActionDescriptor(3585, "Produse_Reclamatii_Capsula_Intense Mixed Berries 18Mg_Inlocuire capsula martor_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3586", new ConsumerActionDescriptor(3586, "Produse_Reclamatii_Capsula_Intense Tobacco 18Mg_Inlocuire capsula martor_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3587", new ConsumerActionDescriptor(3587, "Produse_Reclamatii_Capsula_Intense Chai 18Mg_Inlocuire capsula martor_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3444", new ConsumerActionDescriptor(3444, "Produse_Reclamatii_Capsula_Berry Mint 12mg_Curge lichid din capsula_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3447", new ConsumerActionDescriptor(3447, "Produse_Reclamatii_Capsula_Strawberry 12mg_Curge lichid din capsula_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3448", new ConsumerActionDescriptor(3448, "Produse_Reclamatii_Capsula_Nutty Cream 12mg_Curge lichid din capsula_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3449", new ConsumerActionDescriptor(3449, "Produse_Reclamatii_Capsula_Cherry 12mg_Curge lichid din capsula_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3450", new ConsumerActionDescriptor(3450, "Produse_Reclamatii_Capsula_Tobacco 18mg_Curge lichid din capsula_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3452", new ConsumerActionDescriptor(3452, "Produse_Reclamatii_Capsula_Intense Menthol 18mg_Curge lichid din capsula_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3453", new ConsumerActionDescriptor(3453, "Produse_Reclamatii_Capsula_Intense Mixed Berries 18Mg_Curge lichid din capsula_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3454", new ConsumerActionDescriptor(3454, "Produse_Reclamatii_Capsula_Intense Tobacco 18Mg_Curge lichid din capsula_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3455", new ConsumerActionDescriptor(3455, "Produse_Reclamatii_Capsula_Intense Chai 18Mg_Curge lichid din capsula_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3456", new ConsumerActionDescriptor(3456, "Produse_Reclamatii_Capsula_Berry Mint 12mg_Scurgeri de lichid din capsula inainte de utilizare - detaliere_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3457", new ConsumerActionDescriptor(3457, "Produse_Reclamatii_Capsula_Menthol 18mg_Scurgeri de lichid din capsula inainte de utilizare - detaliere_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3459", new ConsumerActionDescriptor(3459, "Produse_Reclamatii_Capsula_Strawberry 12mg_Scurgeri de lichid din capsula inainte de utilizare - detaliere_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3460", new ConsumerActionDescriptor(3460, "Produse_Reclamatii_Capsula_Nutty Cream 12mg_Scurgeri de lichid din capsula inainte de utilizare - detaliere_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3461", new ConsumerActionDescriptor(3461, "Produse_Reclamatii_Capsula_Cherry 12mg_Scurgeri de lichid din capsula inainte de utilizare - detaliere_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3462", new ConsumerActionDescriptor(3462, "Produse_Reclamatii_Capsula_Tobacco 18mg_Scurgeri de lichid din capsula inainte de utilizare - detaliere_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3464", new ConsumerActionDescriptor(3464, "Produse_Reclamatii_Capsula_Intense Menthol 18mg_Scurgeri de lichid din capsula inainte de utilizare - detaliere_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3465", new ConsumerActionDescriptor(3465, "Produse_Reclamatii_Capsula_Intense Mixed Berries 18Mg_Scurgeri de lichid din capsula inainte de utilizare - detaliere_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3466", new ConsumerActionDescriptor(3466, "Produse_Reclamatii_Capsula_Intense Tobacco 18Mg_Scurgeri de lichid din capsula inainte de utilizare - detaliere_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3477", new ConsumerActionDescriptor(3477, "Produse_Reclamatii_Capsula_Intense Mixed Berries 18Mg_Scurgeri de lichid la prima inhalare_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3480", new ConsumerActionDescriptor(3480, "Produse_Reclamatii_Capsula_Berry Mint 12mg_Scurgeri diverse_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3481", new ConsumerActionDescriptor(3481, "Produse_Reclamatii_Capsula_Menthol 18mg_Scurgeri diverse_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3482", new ConsumerActionDescriptor(3482, "Produse_Reclamatii_Capsula_Toasted Tobacco 12mg_Scurgeri diverse_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3483", new ConsumerActionDescriptor(3483, "Produse_Reclamatii_Capsula_Strawberry 12mg_Scurgeri diverse_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3484", new ConsumerActionDescriptor(3484, "Produse_Reclamatii_Capsula_Nutty Cream 12mg_Scurgeri diverse_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3485", new ConsumerActionDescriptor(3485, "Produse_Reclamatii_Capsula_Cherry 12mg_Scurgeri diverse_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3486", new ConsumerActionDescriptor(3486, "Produse_Reclamatii_Capsula_Tobacco 18mg_Scurgeri diverse_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3488", new ConsumerActionDescriptor(3488, "Produse_Reclamatii_Capsula_Intense Menthol 18mg_Scurgeri diverse_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3489", new ConsumerActionDescriptor(3489, "Produse_Reclamatii_Capsula_Intense Mixed Berries 18Mg_Scurgeri diverse_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3490", new ConsumerActionDescriptor(3490, "Produse_Reclamatii_Capsula_Intense Tobacco 18Mg_Scurgeri diverse_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3491", new ConsumerActionDescriptor(3491, "Produse_Reclamatii_Capsula_Intense Chai 18Mg_Scurgeri diverse_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3492", new ConsumerActionDescriptor(3492, "Produse_Reclamatii_Capsula_Berry Mint 12mg_Defecte de fabrica ale capsulei_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3497", new ConsumerActionDescriptor(3497, "Produse_Reclamatii_Capsula_Cherry 12mg_Defecte de fabrica ale capsulei_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3502", new ConsumerActionDescriptor(3502, "Produse_Reclamatii_Capsula_Intense Tobacco 18Mg_Defecte de fabrica ale capsulei_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3528", new ConsumerActionDescriptor(3528, "Produse_Reclamatii_Capsula_Berry Mint 12mg_Senzatie de gust ars_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3529", new ConsumerActionDescriptor(3529, "Produse_Reclamatii_Capsula_Menthol 18mg_Senzatie de gust ars_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3533", new ConsumerActionDescriptor(3533, "Produse_Reclamatii_Capsula_Cherry 12mg_Senzatie de gust ars_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3539", new ConsumerActionDescriptor(3539, "Produse_Reclamatii_Capsula_Intense Chai 18Mg_Senzatie de gust ars_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3418", new ConsumerActionDescriptor(3418, "Produse_Reclamatii_Device_Slate Grey_Alte probleme alte bateriei_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3473", new ConsumerActionDescriptor(3473, "Produse_Reclamatii_Capsula_Cherry 12mg_Scurgeri de lichid la prima inhalare_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3498", new ConsumerActionDescriptor(3498, "Produse_Reclamatii_Capsula_Tobacco 18mg_Defecte de fabrica ale capsulei_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#3534", new ConsumerActionDescriptor(3534, "Produse_Reclamatii_Capsula_Tobacco 18mg_Senzatie de gust ars_Inlocuire_generated", 3, "2"));
        affinityActions.put("2#1730", new ConsumerActionDescriptor(1730, "Email address confirmed ", 2, "2"));
        affinityActions.put("2#3339", new ConsumerActionDescriptor(3339, "confirm_phone_number", 2, "2"));
    }

    public static int getSkuScore(String productWebSku) {
        //only logic products will use this method
        if(productScoreMap != null){
            return productScoreMap.get(productWebSku);
        }

        synchronized (AffinityActionsDict.class){
            productScoreMap = new HashMap<>();
        }
        String query = "select sku_web, score from affinity.logic_command_content_scoring";
        try(Connection conn  = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query)){
            while(rs.next()){
                String webSku = rs.getString(1);
                int score = rs.getInt(2);
                productScoreMap.put(webSku, score);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            return 0;
        }
        return productScoreMap.get(productWebSku);
    }

    public static String extractTagValue(String jsonStr, String tag) {
        String borderedTag = "\""+tag+"\":";
        int valueStartIdx = jsonStr.indexOf(borderedTag);

        if(valueStartIdx >=0){
            valueStartIdx += borderedTag.length();
            valueStartIdx = jsonStr.indexOf("\"", valueStartIdx);
            int valueEndIdx = jsonStr.indexOf("\"", valueStartIdx+1);
            return jsonStr.substring(valueStartIdx+1, valueEndIdx);
        }else{
            return "";
        }
    }

    public static boolean filter(ConsumerActionsValue cav){
        String systemId = cav.getSystemId()+"";
        String actionId = cav.getActionId()+"";
        final long twoYearsDurationMillis = 365L*24*60*60*1000;
        Date actionDate = new Date(Long.parseLong(cav.getExternalSystemDate()));
        Date now = new Date();
        boolean allowedActionDate = actionDate.after(new Date(System.currentTimeMillis() - twoYearsDurationMillis)) &&
                (actionDate.before(now) || actionDate.equals(now));
        return affinityActions.containsKey(systemId+"#"+actionId) && allowedActionDate;
    }


    public static ConsumerActionDescriptor get(String systemId, String actionId){
        return affinityActions.get(systemId+"#"+actionId);
    }


    public static int getScore(String systemId, String actionId, ConsumerActionsValue.ConsumerActionsPayload consumerActionPayload) {
        ConsumerActionDescriptor affinityAction = get(systemId, actionId);
        if(affinityAction == null){
            return 0;
        }else if(affinityAction.isComplex()){
            return affinityAction.getComplexScoreComputeFunction().apply(consumerActionPayload);
        }else{
            return affinityAction.getScore();
        }
    }
}
