package software.amazon.msk.serverlesscluster;

import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
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
import software.amazon.awssdk.services.kafka.model.BadRequestException;
import software.amazon.awssdk.services.kafka.model.Cluster;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import software.amazon.awssdk.services.kafka.model.DescribeClusterV2Request;
import software.amazon.awssdk.services.kafka.model.DescribeClusterV2Response;
import software.amazon.awssdk.services.kafka.model.ForbiddenException;
import software.amazon.awssdk.services.kafka.model.InternalServerErrorException;
import software.amazon.awssdk.services.kafka.model.KafkaException;
import software.amazon.awssdk.services.kafka.model.ServiceUnavailableException;
import software.amazon.awssdk.services.kafka.model.TagResourceRequest;
import software.amazon.awssdk.services.kafka.model.TagResourceResponse;
import software.amazon.awssdk.services.kafka.model.UnauthorizedException;
import software.amazon.awssdk.services.kafka.model.UntagResourceRequest;
import software.amazon.awssdk.services.kafka.model.UntagResourceResponse;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<KafkaClient> proxyClient;

    @Mock
    KafkaClient kafkaClient;

    private UpdateHandler updateHandler;

    private static Stream<Arguments> requestKafkaErrorToCfnError() {
        return Stream.of(
            arguments(InternalServerErrorException.class, HandlerErrorCode.InternalFailure),
            arguments(ForbiddenException.class, HandlerErrorCode.InvalidRequest),
            arguments(ServiceUnavailableException.class, HandlerErrorCode.ServiceInternalError),
            arguments(UnauthorizedException.class, HandlerErrorCode.InvalidRequest),
            arguments(BadRequestException.class, HandlerErrorCode.InvalidRequest),
            arguments(IllegalArgumentException.class, HandlerErrorCode.InvalidRequest),
            arguments(AwsServiceException.class, HandlerErrorCode.GeneralServiceException));
    }

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        kafkaClient = mock(KafkaClient.class);
        proxyClient = MOCK_PROXY(proxy, kafkaClient);
        updateHandler = new UpdateHandler();
    }

    @AfterEach
    public void tear_down() {
        verifyNoMoreInteractions(kafkaClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        // Given
        final TagResourceResponse tagResourceResponse = TagResourceResponse.builder().build();
        final UntagResourceResponse untagResourceResponse = UntagResourceResponse.builder().build();
        when(proxyClient.client().tagResource(any(TagResourceRequest.class))).thenReturn(tagResourceResponse);
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class))).thenReturn(untagResourceResponse);

        Cluster desiredCluster = getServerlessCluster(ClusterState.ACTIVE);
        final DescribeClusterV2Response describeClusterResponse =
            DescribeClusterV2Response.builder().clusterInfo(desiredCluster).build();
        when(proxyClient.client().describeClusterV2(any(DescribeClusterV2Request.class)))
            .thenReturn(describeClusterResponse);

        // When
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .previousResourceState(buildResourceModelWithTags(TAGS_ALTERED))
            .desiredResourceState(buildResourceModel())
            .clientRequestToken(CLIENT_REQUEST_TOKEN)
            .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = updateHandler.handleRequest(proxy, request,
            new CallbackContext(), proxyClient, logger);

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);

        assertThat(response.getResourceModel().getClusterName())
            .isEqualTo(request.getDesiredResourceState().getClusterName());
        assertThat(response.getResourceModel().getClientAuthentication())
            .isEqualTo(request.getDesiredResourceState().getClientAuthentication());
        assertThat(response.getResourceModel().getVpcConfigs())
            .isEqualTo(request.getDesiredResourceState().getVpcConfigs());
        assertThat(response.getResourceModel().getTags())
            .isEqualTo(request.getDesiredResourceState().getTags());
        assertThat(response.getResourceModel().getArn())
            .isEqualTo(request.getDesiredResourceState().getArn());

        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client()).untagResource(any(UntagResourceRequest.class));
        verify(proxyClient.client()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), times(1)).describeClusterV2(any(DescribeClusterV2Request.class));
        verify(kafkaClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleRequest_withoutPreviousResourceState_throwsCfnInvalidRequestException() {
        // Given
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(buildResourceModel())
            .clientRequestToken(CLIENT_REQUEST_TOKEN)
            .build();

        // When & Then
        assertThatThrownBy(
            () -> updateHandler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger))
            .isInstanceOf(CfnInvalidRequestException.class);
    }

    @ParameterizedTest
    @MethodSource("requestKafkaErrorToCfnError")
    public void handleRequest_untagRequestThrowsException(Class<KafkaException> kafkaException,
                                                          HandlerErrorCode cfnError) {
        // Given
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
            .thenThrow(kafkaException);

        // When
        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(buildResourceModel())
                .desiredResourceState(buildResourceModelWithTags(TAGS_REMOVED))
                .clientRequestToken(CLIENT_REQUEST_TOKEN)
                .previousResourceTags(TAGS)
                .desiredResourceTags(TAGS_REMOVED)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
            updateHandler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(cfnError);

        verify(proxyClient.client()).untagResource(any(UntagResourceRequest.class));
        verify(kafkaClient, atLeastOnce()).serviceName();
    }

    @ParameterizedTest
    @MethodSource("requestKafkaErrorToCfnError")
    public void handleRequest_tagRequestThrowsException(Class<KafkaException> kafkaException,
                                                        HandlerErrorCode cfnError) {
        // Given
        when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
            .thenThrow(kafkaException);

        // When
        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(buildResourceModel())
                .desiredResourceState(buildResourceModelWithTags(TAGS_ADDED))
                .clientRequestToken(CLIENT_REQUEST_TOKEN)
                .previousResourceTags(TAGS)
                .desiredResourceTags(TAGS_ADDED)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
            updateHandler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(cfnError);

        verify(proxyClient.client()).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), times(0)).untagResource(any(UntagResourceRequest.class));
        verify(kafkaClient, atLeastOnce()).serviceName();
    }
}
