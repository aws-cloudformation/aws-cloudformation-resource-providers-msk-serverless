package software.amazon.msk.serverlesscluster;

import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.DescribeClusterV2Request;
import software.amazon.awssdk.services.kafka.model.DescribeClusterV2Response;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandlerStd {
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

        return proxy.initiate("AWS-MSK-ServerlessCluster::Read", proxyClient, model, callbackContext)
            .translateToServiceRequest(Translator::translateToReadRequest)
            .makeServiceCall((describeClusterRequest, sdkProxyClient) -> readResource(describeClusterRequest, sdkProxyClient , clientRequestToken))
            .handleError((describeClusterRequest, exception, _proxyClient, _resourceModel, _callbackContext) ->
                handleError(exception, model,  callbackContext, logger, clientRequestToken))
            .done((describeClusterRequest, describeClusterResponse, proxyInvocation, resourceModel, context) ->
                constructResourceModelFromResponse(describeClusterResponse));
    }

    /**
     * Implement client invocation of the read request through the proxyClient, which is already initialized with
     * caller credentials, correct region and retry settings
     * @param describeClusterRequest the aws service request to describe a resource
     * @param proxyClient the aws service client to make the call
     * @return describe resource response
     */
    private DescribeClusterV2Response readResource(
        final DescribeClusterV2Request describeClusterRequest,
        final ProxyClient<KafkaClient> proxyClient,
        final String clientRequestToken) {

        DescribeClusterV2Response describeClusterResponse =
            proxyClient.injectCredentialsAndInvokeV2(describeClusterRequest, proxyClient.client()::describeClusterV2);

        logger.log(String.format("[ClientRequestToken: %s] Successfully read Cluster %s", clientRequestToken,
            describeClusterRequest.clusterArn()));

        return describeClusterResponse;
    }

    /**
     * Implement client invocation of the read request through the proxyClient, which is already initialized with
     * caller credentials, correct region and retry settings
     * @param describeClusterResponse the aws service describe resource response
     * @return progressEvent indicating success, in progress with delay callback or failed state
     */
    private ProgressEvent<ResourceModel, CallbackContext> constructResourceModelFromResponse(
        final DescribeClusterV2Response describeClusterResponse) {

        return ProgressEvent.defaultSuccessHandler(Translator.translateFromReadResponse(describeClusterResponse));
    }
}
