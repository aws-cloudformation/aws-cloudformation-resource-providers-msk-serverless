package software.amazon.msk.serverlesscluster;

import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.cloudformation.LambdaWrapper;

public class ClientBuilder {

    public static KafkaClient getClient(){
        return KafkaClient.builder()
            .httpClient(LambdaWrapper.HTTP_CLIENT)
            .build();
    }
}
