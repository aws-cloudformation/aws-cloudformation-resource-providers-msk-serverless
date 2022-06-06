package software.amazon.msk.serverlesscluster;

import java.time.Duration;

import org.apache.commons.lang3.StringUtils;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.BadRequestException;
import software.amazon.awssdk.services.kafka.model.DescribeClusterV2Request;
import software.amazon.awssdk.services.kafka.model.DescribeClusterV2Response;
import software.amazon.awssdk.services.kafka.model.ForbiddenException;
import software.amazon.awssdk.services.kafka.model.InternalServerErrorException;
import software.amazon.awssdk.services.kafka.model.NotFoundException;
import software.amazon.awssdk.services.kafka.model.ServiceUnavailableException;
import software.amazon.awssdk.services.kafka.model.TooManyRequestsException;
import software.amazon.awssdk.services.kafka.model.UnauthorizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

// Placeholder for the functionality that could be shared across Create/Read/Update/Delete/List Handlers

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {

    public static final String LOG_MSG_MSK_API_REQUEST_FAILED = "MSK API request failed: %s";
    public static final String LOG_MSG_MSK_SELF_SERVICE_INTERNAL_FAILURE = "MSK Self-Service Internal Failure: %s";
    protected static final String MSK_API_PARAM_NAME_CLUSTERARN = "clusterArn";
    protected static final String INVALID_PARAMETER_EXCEPTION = "One or more of the parameters are not valid";
    protected static final Constant STABILIZATION_DELAY_CREATE =
        Constant.of().timeout(Duration.ofMinutes(120L)).delay(Duration.ofSeconds(30L)).build();
    protected static final Constant STABILIZATION_DELAY_DELETE =
        Constant.of().timeout(Duration.ofMinutes(75L)).delay(Duration.ofSeconds(30L)).build();

    @Override
    public final ProgressEvent<ResourceModel,
        CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {
        return handleRequest(
            proxy,
            request,
            callbackContext != null ? callbackContext : new CallbackContext(),
            proxy.newProxy(ClientBuilder::getClient),
            logger
        );
    }

    protected abstract ProgressEvent<ResourceModel,
        CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<KafkaClient> proxyClient,
        final Logger logger);

    protected ProgressEvent<ResourceModel,
        CallbackContext> describeCluster(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<KafkaClient> proxyClient,
        final ResourceModel model,
        final CallbackContext context,
        final String clientRequestToken,
        final Logger logger) {

        return proxy
            .initiate("AWS-MSK-ServerlessCluster::GetServerlessClusterDetails", proxyClient, model, context)
            .translateToServiceRequest(Translator::translateToReadRequest)
            .makeServiceCall((describeClusterRequest, _proxyClient) ->
                getServerlessClusterDetails(describeClusterRequest, _proxyClient, logger))
            .handleError((describeClusterRequest, exception, _proxyClient, _resourceModel, _callbackContext) ->
                handleError(exception, model, context, logger, clientRequestToken))
            .done(describeClusterResponse -> ProgressEvent.defaultSuccessHandler(
                Translator.translateFromReadResponse(describeClusterResponse)));
    }

    protected ProgressEvent<ResourceModel,
        CallbackContext> handleError(
        final Exception exception,
        final ResourceModel model,
        final CallbackContext callbackContext,
        final Logger logger,
        final String clientRequestToken)
        throws Exception {

        if (exception instanceof IllegalArgumentException) {
            final String exceptionMessage = exception.getMessage();
            logger.log(String.format("[ClientRequestToken: %s] Property validation failure while creating cluster: %s",
                clientRequestToken, exceptionMessage));
            return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InvalidRequest, String.format("%s",
                exceptionMessage));
        }

        if (exception instanceof BadRequestException) {
            logger.log(String.format(LOG_MSG_MSK_API_REQUEST_FAILED, exception.getMessage()));
            return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InvalidRequest, String.format(
                "[ClientRequestToken: %s] %s '%s'", clientRequestToken, exception.getMessage(),
                ((BadRequestException) exception).invalidParameter()));
        }

        if (exception instanceof ForbiddenException || exception instanceof UnauthorizedException) {
            logger.log(String.format(LOG_MSG_MSK_API_REQUEST_FAILED, exception.getMessage()));
            return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InvalidRequest, String.format(
                "[ClientRequestToken: %s] %s", clientRequestToken, exception.getMessage()));
        }

        if (exception instanceof InternalServerErrorException) {
            logger.log(String.format(LOG_MSG_MSK_SELF_SERVICE_INTERNAL_FAILURE, exception.getMessage()));
            return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.InternalFailure, String.format(
                "[ClientRequestToken: %s] %s", clientRequestToken, exception.getMessage()));
        }

        if (exception instanceof ServiceUnavailableException) {
            logger.log(String.format(LOG_MSG_MSK_SELF_SERVICE_INTERNAL_FAILURE, exception.getMessage()));
            return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.ServiceInternalError, String.format(
                "[ClientRequestToken: %s] %s", clientRequestToken, exception.getMessage()));
        }

        if (exception instanceof TooManyRequestsException) {
            logger.log(String.format(LOG_MSG_MSK_API_REQUEST_FAILED, exception.getMessage()));
            return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.Throttling, String.format(
                "[ClientRequestToken: %s] %s", clientRequestToken, exception.getMessage()));
        }

        if (exception instanceof NotFoundException) {
            logger.log(String.format(LOG_MSG_MSK_API_REQUEST_FAILED, exception.getMessage()));
            return ProgressEvent.failed(model, callbackContext, HandlerErrorCode.NotFound, String.format(
                "[ClientRequestToken: %s] %s", clientRequestToken, exception.getMessage()));
        }

        if (exception instanceof AwsServiceException) {
            boolean isError5XX =
                StringUtils.isNotEmpty(exception.getMessage()) &&
                    exception.getMessage().contains("Status Code: 5");
            logger.log(String.format(LOG_MSG_MSK_API_REQUEST_FAILED, exception.getMessage()));

            return ProgressEvent.failed(model, callbackContext,
                isError5XX ?
                    HandlerErrorCode.ServiceInternalError :
                    HandlerErrorCode.GeneralServiceException,
                String.format("[ClientRequestToken: %s] %s", clientRequestToken, exception.getMessage()));
        }

        logger.log(String.format(LOG_MSG_MSK_API_REQUEST_FAILED, exception.getMessage()));
        throw exception;
    }

//    protected ProgressEvent<ResourceModel,
//        CallbackContext> validateCreateRequest(
//        final ResourceModel resourceModel,
//        final CallbackContext callbackContext,
//        final ProgressEvent<ResourceModel, CallbackContext> progressEvent,
//        final Logger logger) {
//        try {
//            validateMTlsProperty(Optional.ofNullable(resourceModel.getClientAuthentication())
//                .map(ClientAuthentication::getTls)
//                .orElse(null));
//        } catch (IllegalArgumentException e) {
//            return ProgressEvent.failed(resourceModel, callbackContext, HandlerErrorCode.InvalidRequest, String.format(
//                "%s '%s'", e.getMessage(), "TLS"));
//        }
//        return ProgressEvent.progress(resourceModel, progressEvent.getCallbackContext());
//    }

    private DescribeClusterV2Response getServerlessClusterDetails(
        final DescribeClusterV2Request describeClusterRequest,
        final ProxyClient<KafkaClient> proxyClient,
        final Logger logger) {

        logger.log(String.format("Fetching cluster of resource %s.", describeClusterRequest.clusterArn()));

        return proxyClient
            .injectCredentialsAndInvokeV2(describeClusterRequest,proxyClient.client()::describeClusterV2);
    }

//    public static void validateMTlsProperty(final Tls mTls) {
//        if (mTls == null || mTls.getEnabled() == null) {
//            return;
//        }
//
//        final boolean isMtlsEnabled = mTls.getEnabled();
//        final boolean isPCAListEmptyOrNull =
//            Optional.ofNullable(mTls.getCertificateAuthorityArnList())
//                .map(List::isEmpty)
//                .orElse(true);
//
//        if ((isMtlsEnabled && isPCAListEmptyOrNull) || (!isMtlsEnabled && !isPCAListEmptyOrNull)) {
//            throw new IllegalArgumentException(INVALID_CLUSTER_PROPERTIES_TLS);
//        }
//    }
}
