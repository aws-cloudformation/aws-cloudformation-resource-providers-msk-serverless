package software.amazon.msk.serverlesscluster;

import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import software.amazon.awssdk.services.kafka.model.KafkaException;
import software.amazon.awssdk.services.kafka.model.ListClustersV2Request;
import software.amazon.awssdk.services.kafka.model.ListClustersV2Response;
import software.amazon.awssdk.services.kafka.model.ServiceUnavailableException;
import software.amazon.awssdk.services.kafka.model.TooManyRequestsException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    KafkaClient kafkaClient;

    @Mock
    private ProxyClient<KafkaClient> proxyClient;

    private ListHandler handler;

    private static Stream<Arguments> KafkaErrorToCfnError() {
        return Stream.of(
            arguments(ServiceUnavailableException.class, HandlerErrorCode.ServiceInternalError),
            arguments(TooManyRequestsException.class, HandlerErrorCode.Throttling),
            arguments(AwsServiceException.class, HandlerErrorCode.GeneralServiceException));
    }

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(
            logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        kafkaClient = mock(KafkaClient.class);
        proxyClient = MOCK_PROXY(proxy, kafkaClient);
        handler = new ListHandler();
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        // Given
        final ListClustersV2Response listClustersResponse =
            ListClustersV2Response.builder().clusterInfoList(getServerlessCluster(ClusterState.ACTIVE)).build();
        when(proxyClient.client().listClustersV2(any(ListClustersV2Request.class)))
            .thenReturn(listClustersResponse);

        final ResourceModel model = ResourceModel.builder().build();

        // When
        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(model)
                .clientRequestToken(CLIENT_REQUEST_TOKEN).build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels().size()).isEqualTo(1);
        assertThat(response.getResourceModels().get(0).getArn()).isEqualTo(CLUSTER_ARN);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).listClustersV2(any(ListClustersV2Request.class));
    }

    @ParameterizedTest
    @MethodSource("KafkaErrorToCfnError")
    public void handleRequest_Exception(Class<KafkaException> kafkaException, HandlerErrorCode cfnError) {
        // Given
        when(proxyClient.client().listClustersV2(any(ListClustersV2Request.class))).thenThrow(kafkaException);

        final ResourceModel model = ResourceModel.builder().build();

        // When
        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(model)
                .clientRequestToken(CLIENT_REQUEST_TOKEN).build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getNextToken()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(cfnError);

        verify(proxyClient.client()).listClustersV2(any(ListClustersV2Request.class));
    }
}
