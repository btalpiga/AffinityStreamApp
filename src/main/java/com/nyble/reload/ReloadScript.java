//package com.nyble.reload;
//
//import com.nyble.topics.Names;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.net.HttpURLConnection;
//import java.net.MalformedURLException;
//import java.net.URI;
//import java.net.URL;
//import java.net.http.HttpClient;
//
//public class ReloadScript {
//
//    public static void run() throws IOException {
//        checkKafkaConnectActionsPaused();
//        checkAppNotRunning();
//        deleteStartConnector("jdbc_source_consumer_affinity_score_start");
//        emptyIntermediateTopics();
//        resetStreamApp("affinity-stream");
//        deleteInternalStreamTopics("affinity-stream");
//
//        final int maxRmc = getMaxId(Names.RMC_SYSTEM_ID);
//        final int maxRrp = getMaxId(Names.RRP_SYSTEM_ID);
//        final String initialAffinityCalcSql = getReloadScript(maxRmc, maxRrp);
//        System.out.println("**Done (max_id_rmc = "+maxRmc+" && max_id_rrp = "+maxRrp+")\n" +
//                "Steps remained: \n" +
//                "\t0.\tUpdate max ids in AffinityStreamApp and rebuild it.\n"+
//                "\t1.\tStart AffinityStreamApp.jar\n" +
//                "\t2.\tCreate jdbc_source_consumer_affinity_score_start kafka connector using postman (request attached in sources)\n" +
//                "\t3.\tWait for kafka connector to load and than delete it.");
//    }
//
//    private static void checkKafkaConnectActionsPaused() throws IOException {
//        final String actionsRmc = "jdbc_source_consumer_actions_rmc";
//        final String actionsRrp = "jdbc_source_consumer_actions_rrp";
//
//        URL url = new URL(String.format("http://10.100.1.17:8083/connectors/%s/pause", actionsRmc));
//        HttpURLConnection http = (HttpURLConnection) url.openConnection();
//        http.setRequestMethod("PUT");
//        if(http.getResponseCode() != 202){
//            throw new RuntimeException("Could not stop "+actionsRmc);
//        }
//
//        url = new URL(String.format("http://10.100.1.17:8083/connectors/%s/pause", actionsRrp));
//        http = (HttpURLConnection) url.openConnection();
//        http.setRequestMethod("PUT");
//        if(http.getResponseCode() != 202){
//            throw new RuntimeException("Could not stop "+actionsRrp);
//        }
//    }
//
//    private static String getAllConnectors() throws IOException {
//        URL url = new URL("http://10.100.1.17:8083/connectors");
//        HttpURLConnection http = (HttpURLConnection) url.openConnection();
//        http.setRequestMethod("GET");
//        try(BufferedReader br = new BufferedReader(new InputStreamReader(http.getInputStream()))){
//            String line;
//            StringBuffer sb = new StringBuffer();
//            while((line = br.readLine())!=null){
//                sb.append(line).append("\n");
//            }
//            return sb.toString();
//        }
//    }
//
//}
