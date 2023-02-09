package software.amazon.msk.serverlesscluster;

import java.util.Map;
import java.util.Set;

import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandlerStd {
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<KafkaClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        final ResourceModel resourceModel = request.getDesiredResourceState();
        final String clientRequestToken = request.getClientRequestToken();

        if(request.getPreviousResourceState() == null) {
            throw new CfnInvalidRequestException("PreviousResourceState is required.");
        }

        final Map<String, String> previousTags = TagHelper.getPreviouslyAttachedTags(request);
        final Map<String, String> desiredTags = TagHelper.getNewDesiredTags(request);
        final Map<String, String> addedTags = TagHelper.generateTagsToAdd(previousTags, desiredTags);
        final Set<String> removedTags = TagHelper.generateTagsToRemove(previousTags, desiredTags);

        return ProgressEvent.progress(resourceModel, callbackContext)
            .then(progress -> untagResource(proxy, proxyClient, resourceModel, request, callbackContext, progress,
                clientRequestToken, removedTags))
            .then(progress -> tagResource(proxy, proxyClient, resourceModel, request, callbackContext, progress,
                clientRequestToken, addedTags))
            .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    /**
     * tagResource during update
     *
     * Calls the kafka:TagResource API.
     */
    private ProgressEvent<ResourceModel, CallbackContext>
    tagResource(final AmazonWebServicesClientProxy proxy, final ProxyClient<KafkaClient> serviceClient,
                final ResourceModel resourceModel,
                final ResourceHandlerRequest<ResourceModel> handlerRequest, final CallbackContext callbackContext,
                final ProgressEvent<ResourceModel, CallbackContext> progressEvent, final String clientRequestToken,
                final Map<String, String> addedTags) {
        if (addedTags.isEmpty()) {
            return ProgressEvent.progress(resourceModel, progressEvent.getCallbackContext());
        }

        logger.log(String.format("Going to add tags for MSK ServerlessCluster resource: " +
            "%s with AccountId: %s", resourceModel.getClusterName(), handlerRequest.getAwsAccountId()));

        return proxy.initiate("AWS-MSK-ServerlessCluster::TagResource", serviceClient, resourceModel, callbackContext)
            .translateToServiceRequest(model ->
                Translator.translateToTagResourceRequest(resourceModel, addedTags))
            .makeServiceCall((tagResourceRequest, _proxyClient) -> _proxyClient.injectCredentialsAndInvokeV2(
                tagResourceRequest, _proxyClient.client()::tagResource))
            .handleError((tagResourceRequest, exception, _proxyClient, _resourceModel, _callbackContext) ->
                handleError(exception, resourceModel,  callbackContext, logger, clientRequestToken))
            .progress();
    }

    /**
     * untagResource during update
     *
     * Calls the kafka:UntagResource API.
     */
    private ProgressEvent<ResourceModel, CallbackContext>
    untagResource(final AmazonWebServicesClientProxy proxy, final ProxyClient<KafkaClient> serviceClient,
                  final ResourceModel resourceModel,
                  final ResourceHandlerRequest<ResourceModel> handlerRequest, final CallbackContext callbackContext,
                  final ProgressEvent<ResourceModel, CallbackContext> progressEvent, final String clientRequestToken,
                  final Set<String> removedTags) {
        if (removedTags.isEmpty()) {
            return ProgressEvent.progress(resourceModel, progressEvent.getCallbackContext());
        }

        logger.log(String.format("Going to remove tags for MSK ServerlessCluster resource: " +
            "%s with AccountId: %s", resourceModel.getClusterName(), handlerRequest.getAwsAccountId()));

        return proxy.initiate("AWS-MSK-ServerlessCluster::UntagResource", serviceClient, resourceModel, callbackContext)
            .translateToServiceRequest(model ->
                Translator.translateToUntagResourceRequest(model, removedTags))
            .makeServiceCall((untagResourceRequest, _proxyClient) -> _proxyClient.injectCredentialsAndInvokeV2(
                untagResourceRequest, _proxyClient.client()::untagResource))
            .handleError((untagResourceRequest, exception, _proxyClient, _resourceModel, _callbackContext) ->
                handleError(exception, resourceModel,  callbackContext, logger, clientRequestToken))
            .progress();
    }
}
