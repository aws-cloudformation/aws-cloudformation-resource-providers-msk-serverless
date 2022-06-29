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
import software.amazon.awssdk.services.kafka.model.BadRequestException;
import software.amazon.awssdk.services.kafka.model.Cluster;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import software.amazon.awssdk.services.kafka.model.DeleteClusterRequest;
import software.amazon.awssdk.services.kafka.model.DeleteClusterResponse;
import software.amazon.awssdk.services.kafka.model.DescribeClusterV2Request;
import software.amazon.awssdk.services.kafka.model.DescribeClusterV2Response;
import software.amazon.awssdk.services.kafka.model.ForbiddenException;
import software.amazon.awssdk.services.kafka.model.InternalServerErrorException;
import software.amazon.awssdk.services.kafka.model.KafkaException;
import software.amazon.awssdk.services.kafka.model.NotFoundException;
import software.amazon.awssdk.services.kafka.model.ServiceUnavailableException;
import software.amazon.awssdk.services.kafka.model.UnauthorizedException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.msk.serverlesscluster.BaseHandlerStd.INVALID_PARAMETER_EXCEPTION;
import static software.amazon.msk.serverlesscluster.BaseHandlerStd.MSK_API_PARAM_NAME_CLUSTERARN;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<KafkaClient> proxyClient;

    @Mock
    KafkaClient kafkaClient;

    private DeleteHandler handler;

    private static Stream<Arguments> requestKafkaErrorToCfnError() {
        return Stream.of(
            arguments(InternalServerErrorException.class, HandlerErrorCode.InternalFailure),
            arguments(ForbiddenException.class, HandlerErrorCode.InvalidRequest),
            arguments(ServiceUnavailableException.class, HandlerErrorCode.ServiceInternalError),
            arguments(UnauthorizedException.class, HandlerErrorCode.InvalidRequest),
            arguments(IllegalArgumentException.class, HandlerErrorCode.InvalidRequest),
            arguments(AwsServiceException.class, HandlerErrorCode.GeneralServiceException));
    }

    private static Stream<Arguments> stabilizeKafkaErrorToCfnError() {
        return Stream.of(
            arguments(ServiceUnavailableException.class, HandlerErrorCode.ServiceInternalError),
            arguments(InternalServerErrorException.class, HandlerErrorCode.InternalFailure),
            arguments(AwsServiceException.class, HandlerErrorCode.GeneralServiceException));
    }

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        kafkaClient = mock(KafkaClient.class);
        proxyClient = MOCK_PROXY(proxy, kafkaClient);
        handler = new DeleteHandler();
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        // Given
        final DeleteClusterResponse deleteClusterResponse = DeleteClusterResponse.builder().build();
        when(proxyClient.client().deleteCluster(any(DeleteClusterRequest.class)))
            .thenReturn(deleteClusterResponse);

        final DescribeClusterV2Response deletingStateDescribeClusterResponse =
            DescribeClusterV2Response.builder().clusterInfo(getServerlessCluster(ClusterState.DELETING)).build();
        when(proxyClient.client().describeClusterV2(any(DescribeClusterV2Request.class)))
            .thenReturn(deletingStateDescribeClusterResponse)
            .thenThrow(NotFoundException.class);

        final ResourceModel model = ResourceModel.builder().clusterName(CLUSTER_NAME).build();

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
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client()).deleteCluster(any(DeleteClusterRequest.class));
        verify(proxyClient.client(), times(2)).describeClusterV2(any(DescribeClusterV2Request.class));
    }

    @Test
    public void handleRequest_BadRequest_InvalidClusterArn_Success() {
        // Given
        final DeleteClusterResponse deleteClusterResponse = DeleteClusterResponse.builder().build();
        when(proxyClient.client().deleteCluster(any(DeleteClusterRequest.class)))
            .thenReturn(deleteClusterResponse);

        when(proxyClient.client().describeClusterV2(any(DescribeClusterV2Request.class)))
            .thenThrow(BadRequestException.builder().invalidParameter(MSK_API_PARAM_NAME_CLUSTERARN)
                .message(INVALID_PARAMETER_EXCEPTION).build());

        final ResourceModel model = ResourceModel.builder().clusterName(CLUSTER_NAME).build();

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
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).deleteCluster(any(DeleteClusterRequest.class));
        verify(proxyClient.client(), times(1)).describeClusterV2(any(DescribeClusterV2Request.class));
    }

    @ParameterizedTest
    @MethodSource("requestKafkaErrorToCfnError")
    public void handleRequest_Exception(Class<KafkaException> kafkaException, HandlerErrorCode cfnError) {
        // Given
        when(proxyClient.client().deleteCluster(any(DeleteClusterRequest.class)))
            .thenThrow(kafkaException);

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
        assertThat(response.getErrorCode()).isEqualTo(cfnError);

        verify(proxyClient.client()).deleteCluster(any(DeleteClusterRequest.class));
    }

    @Test
    public void handleStabilize_UnexpectedStatus() {
        // Given
        final DeleteClusterResponse deleteClusterResponse = DeleteClusterResponse.builder().build();
        when(proxyClient.client().deleteCluster(any(DeleteClusterRequest.class)))
            .thenReturn(deleteClusterResponse);

        DescribeClusterV2Response describeClusterResponse =
            DescribeClusterV2Response.builder()
                .clusterInfo(Cluster.builder().state(ClusterState.FAILED).build()).build();
        when(proxyClient.client().describeClusterV2(any(DescribeClusterV2Request.class)))
            .thenReturn(describeClusterResponse);

        final ResourceModel model = ResourceModel.builder().clusterName(CLUSTER_NAME).build();

        // When & Then
        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(model)
                .clientRequestToken(CLIENT_REQUEST_TOKEN).build();

        assertThrows(CfnNotStabilizedException.class,
            () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client()).deleteCluster(any(DeleteClusterRequest.class));
        verify(proxyClient.client()).describeClusterV2(any(DescribeClusterV2Request.class));
    }

    @Test
    public void handleStabilize_BadRequest_InvalidParameter() {
        // Given
        when(proxyClient.client().deleteCluster(any(DeleteClusterRequest.class)))
            .thenReturn(DeleteClusterResponse.builder().build());

        when(proxyClient.client().describeClusterV2(any(DescribeClusterV2Request.class)))
            .thenThrow(BadRequestException.class);

        final ResourceModel model = ResourceModel.builder().clusterName(CLUSTER_NAME).build();

        // When & Then
        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(model)
                .clientRequestToken(CLIENT_REQUEST_TOKEN).build();

        assertThrows(CfnInvalidRequestException.class,
            () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client()).deleteCluster(any(DeleteClusterRequest.class));
        verify(proxyClient.client()).describeClusterV2(any(DescribeClusterV2Request.class));
    }

    @Test
    public void handleDelete_ResourceNotFound_AlreadyDeletedFailure() {
        // Given
        when(proxyClient.client().deleteCluster(any(DeleteClusterRequest.class)))
            .thenThrow(NotFoundException.class);

        // When & Then
        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(buildResourceModel())
                .clientRequestToken(CLIENT_REQUEST_TOKEN)
                .build();

        assertThrows(CfnNotFoundException.class,
            () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client()).deleteCluster(any(DeleteClusterRequest.class));
        verify(kafkaClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleDelete_BadRequest_InvalidClusterArnFailure() {
        // Given
        when(proxyClient.client().deleteCluster(any(DeleteClusterRequest.class)))
            .thenThrow(BadRequestException.builder().invalidParameter(MSK_API_PARAM_NAME_CLUSTERARN)
                .message(INVALID_PARAMETER_EXCEPTION).build());

        // When & Then
        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(buildResourceModel())
                .clientRequestToken(CLIENT_REQUEST_TOKEN)
                .build();

        assertThrows(CfnNotFoundException.class,
            () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client()).deleteCluster(any(DeleteClusterRequest.class));
        verify(kafkaClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleDelete_BadRequest_InvalidParamFailure_NoMessage() {
        // Given
        when(proxyClient.client().deleteCluster(any(DeleteClusterRequest.class)))
            .thenThrow(BadRequestException.builder().invalidParameter(MSK_API_PARAM_NAME_CLUSTERARN).build());

        // When & Then
        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(buildResourceModel())
                .clientRequestToken(CLIENT_REQUEST_TOKEN)
                .build();

        assertThrows(CfnInvalidRequestException.class,
            () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client()).deleteCluster(any(DeleteClusterRequest.class));
        verify(kafkaClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleDelete_BadRequest_InvalidParamFailure_NotInvalidMessage() {
        // Given
        when(proxyClient.client().deleteCluster(any(DeleteClusterRequest.class)))
            .thenThrow(BadRequestException.builder().invalidParameter(MSK_API_PARAM_NAME_CLUSTERARN)
                .message("unknown").build());

        // When & Then
        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(buildResourceModel())
                .clientRequestToken(CLIENT_REQUEST_TOKEN)
                .build();

        assertThrows(CfnInvalidRequestException.class,
            () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client()).deleteCluster(any(DeleteClusterRequest.class));
        verify(kafkaClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleDelete_BadRequest_InvalidParamFailure() {
        // Given
        when(proxyClient.client().deleteCluster(any(DeleteClusterRequest.class)))
            .thenThrow(BadRequestException.class);

        // When & Then
        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(buildResourceModel())
                .clientRequestToken(CLIENT_REQUEST_TOKEN)
                .build();

        assertThrows(CfnInvalidRequestException.class,
            () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

        verify(proxyClient.client()).deleteCluster(any(DeleteClusterRequest.class));
        verify(kafkaClient, atLeastOnce()).serviceName();
    }

    @ParameterizedTest
    @MethodSource("stabilizeKafkaErrorToCfnError")
    public void handleStabilize_Exception(
        Class<KafkaException> kafkaException, HandlerErrorCode cfnError) {

        final DeleteClusterResponse deleteClusterResponse = DeleteClusterResponse.builder().build();
        when(proxyClient.client().deleteCluster(any(DeleteClusterRequest.class)))
            .thenReturn(deleteClusterResponse);

        when(proxyClient.client().describeClusterV2(any(DescribeClusterV2Request.class)))
            .thenThrow(kafkaException);

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request =
            ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(model)
                .clientRequestToken(CLIENT_REQUEST_TOKEN).build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(cfnError);

        verify(proxyClient.client()).deleteCluster(any(DeleteClusterRequest.class));
        verify(proxyClient.client()).describeClusterV2(any(DescribeClusterV2Request.class));
    }
}
