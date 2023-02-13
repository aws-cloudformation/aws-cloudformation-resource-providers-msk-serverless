package software.amazon.msk.serverlesscluster;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.kafka.KafkaClient;

import static org.assertj.core.api.Assertions.assertThat;

public class ClientBuilderTest extends AbstractTestBase {

    @Test
    public void test_getKafkaClient() {
        // When
        KafkaClient kafkaClient = ClientBuilder.getClient();
        // Then
        assertThat(kafkaClient).isNotNull();
    }
}
