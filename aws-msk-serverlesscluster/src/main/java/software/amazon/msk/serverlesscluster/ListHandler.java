package software.amazon.msk.serverlesscluster;

import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<KafkaClient> proxyClient,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        final String clientRequestToken = request.getClientRequestToken();

        return proxy
            .initiate("AWS-MSK-ServerlessCluster::List", proxyClient, model, callbackContext)
            .translateToServiceRequest(
                _resourceModel -> Translator.translateToListRequest(request.getNextToken()))
            .makeServiceCall(
                (listClustersRequest, _proxyClient) ->
                    _proxyClient.injectCredentialsAndInvokeV2(
                        listClustersRequest, _proxyClient.client()::listClustersV2))
            .handleError((listClustersRequest, exception, _proxyClient, _resourceModel, _callbackContext) ->
                handleError(exception, model,  callbackContext, logger, clientRequestToken))
            .done((listClustersRequest, listClustersResponse, proxyInvocation, resourceModel, context) ->
                ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModels(Translator.translateFromListResponse(listClustersResponse))
                    .status(OperationStatus.SUCCESS)
                    .nextToken(listClustersResponse.nextToken()).build());
    }
}
