import com.google.gson.Gson;
import com.nyble.main.AffinityActionsDict;
import junit.framework.TestCase;

import java.util.List;
import java.util.Map;

public class AffinityActionsDictTest extends TestCase {

    public void test_extractTag(){
        String body = "{\"key1\": \"aaa\", \"key2\":\"bbb\"}";
        String k1val = AffinityActionsDict.extractTagValue(body, "key1");
        String k2val = AffinityActionsDict.extractTagValue(body, "key2");
        String k3val = AffinityActionsDict.extractTagValue(body, "key3");

        assertEquals("aaa", k1val);
        assertEquals("bbb", k2val);
        assertEquals("", k3val);
    }

    public void test_rawJsonStringToMap(){
        String jsonStr = "{\"value\":{\"Status\": \"processing\", \"Invoice\": {\"InvoiceNo\": \"4443\", \"InvoiceDate\": \"03/09/2020\", \"InvoiceValue\": \"110\", \"InvoiceStatus\": \"Pending\"}, \"OrderId\": \"19000033853\", \"Customer\": {\"Email\": \"florin170792@outlook.com\", \"OptinDM\": \"No\", \"OptinMR\": \"No\", \"LastName\": \"Lazar\", \"OptinSMS\": \"No\", \"Birthdate\": \"17/07/1992\", \"FirstName\": \"Florin\", \"CustomerID\": \"79399\", \"OptinEmail\": \"Yes\"}, \"Products\": {\"Product\": [{\"VAT\": \"15.17\", \"Value\": \"79.83\", \"Quantity\": \"1\", \"ProductID\": \"232\", \"ProductSKU\": \"14563804\", \"ProductName\": \"STARTER KIT MENTHOL\", \"SellingPrice\": \"79.83\", \"StandardPrice\": \"79.83\"}]}, \"CreatedDate\": \"03/09/2020 01:37\", \"UpdatedDate\": \"03/09/2020 01:37\", \"custom_info\": \"\", \"TotalWithVAT\": \"110\", \"TrackingInfo\": \"\", \"PaymentMethod\": \"Cash On Delivery\", \"ShippingValue\": \"15\", \"BillingAddress\": {\"City\": \"Alexandria\", \"Phone\": \"0799868454\", \"County\": \"Teleorman\", \"LastName\": \"Lazar\", \"AddressID\": \"146506\", \"FirstName\": \"Florin\", \"PostalCode\": \"140033\", \"StreetAddress\": \"Str. Dunarii nr.220 bl BM1 parter ( banca transilvania ), Str Negru Voda nr.89 bl 413 sc A et2 ap 11\"}, \"ShippingMethod\": \"Curier rapid - Fan Courier\", \"ShippingAddress\": {\"City\": \"Alexandria\", \"Phone\": \"0799868454\", \"County\": \"Teleorman\", \"LastName\": \"Lazar\", \"AddressID\": \"146503\", \"FirstName\": \"Florin\", \"PostalCode\": \"140033\", \"StreetAddress\": \"Str. Dunarii nr.220 bl BM1 parter ( banca transilvania ), Str Negru Voda nr.89 bl 413 sc A et2 ap 11\"}, \"TotalWithoutVAT\": \"92.44\", \"TotalProductsValue\": \"95\"}}";
        Gson gson = new Gson();
        Map<String, Object> payload = gson.fromJson(jsonStr, Map.class);
        Map<String, Object> command = (Map<String, Object>) payload.get("value");
        List<Map<String, Object>> products = (List<Map<String, Object>>) ((Map)(command.get("Products"))).get("Product");

        assertEquals(products.size(), 1);
        assertEquals(products.get(0).get("ProductSKU").toString(), "14563804" );
        assertEquals(products.get(0).get("Quantity").toString(), "1" );
    }
}
