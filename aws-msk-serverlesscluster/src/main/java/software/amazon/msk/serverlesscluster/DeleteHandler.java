package software.amazon.msk.serverlesscluster;

import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.BadRequestException;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import software.amazon.awssdk.services.kafka.model.DeleteClusterRequest;
import software.amazon.awssdk.services.kafka.model.DeleteClusterResponse;
import software.amazon.awssdk.services.kafka.model.NotFoundException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandlerStd {
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

        return ProgressEvent.progress(model, callbackContext)
            .then(progress ->
                proxy.initiate("AWS-MSK-ServerlessCluster::Delete", proxyClient, model, callbackContext)
                    .translateToServiceRequest(Translator::translateToDeleteRequest)
                    .backoffDelay(STABILIZATION_DELAY_DELETE)
                    .makeServiceCall(this::deleteResource)
                    .stabilize(this::stabilizedOnDelete)
                    .handleError((deleteClusterRequest, exception, _proxyClient, _resourceModel, _callbackContext) ->
                        handleError(exception, model,  callbackContext, logger, clientRequestToken))
                    .success());
    }

    /**
     * Implement client invocation of the delete request through the proxyClient, which is already initialized with
     * caller credentials, correct region and retry settings
     * @param deleteClusterRequest the aws service request to delete a resource
     * @param kafkaClient the aws service client to make the call
     * @return delete resource response
     */
    private DeleteClusterResponse deleteResource(
        final DeleteClusterRequest deleteClusterRequest,
        final ProxyClient<KafkaClient> kafkaClient) {
        final String clusterArn = deleteClusterRequest.clusterArn();
        try {
            return kafkaClient.injectCredentialsAndInvokeV2(deleteClusterRequest, kafkaClient.client()::deleteCluster);
        } catch (NotFoundException e) {
            logger.log(String.format("MSK API request for cluster deletion failed with message: %s, because the " +
                "cluster %s does not exist", e.getMessage(), clusterArn));
            throw new CfnNotFoundException(e);
        } catch (BadRequestException e) {
            if (MSK_API_PARAM_NAME_CLUSTERARN.equals(e.invalidParameter()) && e.getMessage() != null
                && e.getMessage().contains(INVALID_PARAMETER_EXCEPTION)) {
                logger.log(String.format("MSK API request for cluster deletion failed due to invalid clusterArn %s " +
                    "with Exception %s", clusterArn, e.getMessage()));
                throw new CfnNotFoundException(e);
            } else {
                // During the cases when the BadRequestException is occurring because of any invalid parameter
                // other than an invalid ClusterArn, we retain the regular behaviour for handling BadRequestException
                logger.log(String.format("MSK API request for cluster deletion failed due to invalid clusterArn %s " +
                    "with Exception %s", clusterArn, e.getMessage()));
                throw new CfnInvalidRequestException(e);
            }
        }
    }

    /**
     * If deletion of your resource requires some form of stabilization (e.g. propagation delay)
     * for more information -> https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-test-contract.html
     * @param deleteClusterRequest the aws service request to delete a resource
     * @param deleteClusterResponse the aws service response to delete a resource
     * @param proxyClient the aws service client to make the call
     * @param model resource model
     * @param callbackContext callback context
     * @return boolean state of stabilized or not
     */
    private boolean stabilizedOnDelete(
        final DeleteClusterRequest deleteClusterRequest,
        final DeleteClusterResponse deleteClusterResponse,
        final ProxyClient<KafkaClient> proxyClient,
        final ResourceModel model,
        final CallbackContext callbackContext) {

        final String clusterArn = deleteClusterRequest.clusterArn();

        try {
            ClusterState currentClusterState =
                proxyClient.injectCredentialsAndInvokeV2(Translator.translateToReadRequest(model),
                    proxyClient.client()::describeClusterV2).clusterInfo().state();

            switch (currentClusterState) {
                case DELETING:
                    logger.log(String.format("Cluster %s is deleting, current state is %s", clusterArn,
                        currentClusterState));
                    return false;
                default:
                    logger.log(String.format("Cluster %s reached unexpected state %s", clusterArn,
                        currentClusterState));
                    throw new CfnNotStabilizedException(
                        ResourceModel.TYPE_NAME, model.getArn());
            }
        } catch (NotFoundException e) {
            logger.log(String.format("Cluster %s is deleted", clusterArn));
            return true;
        } catch (BadRequestException e) {
            if (MSK_API_PARAM_NAME_CLUSTERARN.equals(e.invalidParameter()) && e.getMessage() != null
                && e.getMessage().contains(INVALID_PARAMETER_EXCEPTION)) {
                return true;
            } else {
                throw new CfnInvalidRequestException(e);
            }
        }
    }
}
