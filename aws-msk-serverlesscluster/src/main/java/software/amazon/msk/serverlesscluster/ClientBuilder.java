package software.amazon.msk.serverlesscluster;

import java.net.URI;

import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.cloudformation.LambdaWrapper;

public class ClientBuilder {

    public static KafkaClient getClient(){
        try {
            URI endpoint = new URI("https://kafka-beta.us-east-1.amazonaws.com/");
            return KafkaClient.builder()
                .httpClient(LambdaWrapper.HTTP_CLIENT)
                .endpointOverride(endpoint)
                .build();
        } catch (Exception e) {
            return null;
        }
//
//        return KafkaClient.builder()
//            .httpClient(LambdaWrapper.HTTP_CLIENT)
//            .build();
    }

}
