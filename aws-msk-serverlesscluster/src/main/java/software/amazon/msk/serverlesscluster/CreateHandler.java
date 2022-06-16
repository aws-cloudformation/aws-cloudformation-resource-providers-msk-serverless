package software.amazon.msk.serverlesscluster;

import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import software.amazon.awssdk.services.kafka.model.ConflictException;
import software.amazon.awssdk.services.kafka.model.CreateClusterV2Request;
import software.amazon.awssdk.services.kafka.model.CreateClusterV2Response;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;


public class CreateHandler extends BaseHandlerStd {
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<KafkaClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        final ResourceModel model = request.getDesiredResourceState();
        final String clientRequestToken = request.getClientRequestToken();

        logger.log( String.format("[Request: %s] Handling create operation, resource model: %s", clientRequestToken,
            model));

        model.setTags(request.getDesiredResourceTags());

        return ProgressEvent.progress(model, callbackContext)
            .then(progress ->
                proxy.initiate("AWS-MSK-ServerlessCluster::Create", proxyClient, model, callbackContext)
                    .translateToServiceRequest(Translator::translateToCreateRequest)
                    .backoffDelay(STABILIZATION_DELAY_CREATE)
                    .makeServiceCall(this::createResource)
                    .stabilize(this::stabilizedOnCreate)
                    .handleError((createClusterRequest, exception, _proxyClient, _resourceModel, _callbackContext) ->
                        handleError(exception, model,  callbackContext, logger, clientRequestToken))
                    .progress())
            .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    /**
     * Handler execute operation to call create cluster api
     * @param createClusterRequest the aws service request to create a resource
     * @param proxyClient the aws service client to make the call
     * @return awsResponse create resource response
     */
    private CreateClusterV2Response createResource(
        final CreateClusterV2Request createClusterRequest,
        final ProxyClient<KafkaClient> proxyClient) {
        try {
            return proxyClient
                .injectCredentialsAndInvokeV2(createClusterRequest,proxyClient.client()::createClusterV2);
        } catch (final ConflictException e) {
            logger.log(String.format("Cluster with name %s already exists: %s ", createClusterRequest.clusterName(),
                e.getMessage()));
            throw new CfnAlreadyExistsException(ResourceModel.TYPE_NAME, createClusterRequest.clusterName(), e);
        }
    }

    /**
     * Handler stabilize operation to wait till resource reaches terminal state by calling DescribeClusterV2 api
     * @param createClusterRequest the aws service request to create a resource
     * @param createClusterResponse the aws service response to create a resource
     * @param proxyClient the aws service client to make the call
     * @param model resource model
     * @param callbackContext callback context
     * @return boolean state of stabilized or not
     */
    private boolean stabilizedOnCreate(
        final CreateClusterV2Request createClusterRequest,
        final CreateClusterV2Response createClusterResponse,
        final ProxyClient<KafkaClient> proxyClient,
        final ResourceModel model,
        final CallbackContext callbackContext) {

        if (model.getArn() == null) {
            model.setArn(createClusterResponse.clusterArn());
        }

        final String clusterArn = model.getArn();
        final ClusterState currentClusterState =
            proxyClient.injectCredentialsAndInvokeV2(Translator.translateToReadRequest(model),
                proxyClient.client()::describeClusterV2).clusterInfo().state();

        switch (currentClusterState) {
            case ACTIVE:
                logger.log(String.format("Cluster %s is stabilized, current state is %s", clusterArn,
                    currentClusterState));
                return true;
            case CREATING:
                logger.log(String.format("Cluster %s is stabilizing, current state is %s", clusterArn,
                    currentClusterState));
                return false;
            default:
                logger.log(String.format("Cluster %s reached unexpected state %s", clusterArn,
                    currentClusterState));
                throw new CfnNotStabilizedException(ResourceModel.TYPE_NAME, model.getArn());
        }
    }
}
