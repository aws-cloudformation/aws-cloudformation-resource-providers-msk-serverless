package software.amazon.msk.serverlesscluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.Cluster;
import software.amazon.awssdk.services.kafka.model.ClusterState;
import software.amazon.awssdk.services.kafka.model.ClusterType;
import software.amazon.awssdk.services.kafka.model.Serverless;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProxyClient;

public class AbstractTestBase {
    protected static final Credentials MOCK_CREDENTIALS;
    protected static final LoggerProxy logger;

    protected static final String CLIENT_REQUEST_TOKEN = "ClientToken";
    protected static final String CLUSTER_NAME = "ClusterName";
    protected static final String CLUSTER_ARN = "arn:aws:kafka:us-west-2:083674906042:cluster/ClusterName";
    protected static final Set<String> SECURITY_GROUP_IDS = Sets.newHashSet("SecurityGroup");
    protected static final Set<String> SUBNET_IDS =  Sets.newHashSet("Subnets");
    protected static final ClientAuthentication CLIENT_AUTHENTICATION =
        ClientAuthentication.builder()
            .sasl(Sasl.builder().iam(Iam.builder().enabled(true).build()).build()).build();
    protected static final List<VpcConfig> VPC_CONFIG_LIST = Lists.newArrayList(
        VpcConfig.builder().subnetIds(SUBNET_IDS).securityGroups(SECURITY_GROUP_IDS).build());
    protected static final Map<String, String> TAGS = new HashMap<String, String>() {
        {
            put("TEST_TAG1", "TEST_TAG_VALUE1");
            put("TEST_TAG2", "TEST_TAG_VALUE2");
        }
    };
    protected static final Map<String, String> TAGS_ADDED = new HashMap<String, String>() {
        {
            put("TEST_TAG1", "TEST_TAG_VALUE1");
            put("TEST_TAG2", "TEST_TAG_VALUE2");
            put("TEST_TAG3", "TEST_TAG_VALUE3");
        }
    };
    protected static final Map<String, String> TAGS_REMOVED = new HashMap<String, String>() {
        {
            put("TEST_TAG1", "TEST_TAG_VALUE1");
        }
    };
    protected static final Map<String, String> TAGS_ALTERED = new HashMap<String, String>() {
        {
            put("TEST_TAG1", "TEST_TAG_VALUE1");
            put("TEST_TAG3", "TEST_TAG_VALUE3");
        }
    };

    protected static final software.amazon.awssdk.services.kafka.model.ServerlessClientAuthentication
        CLIENT_AUTHENTICATION_RESPONSE =
        software.amazon.awssdk.services.kafka.model.ServerlessClientAuthentication.builder()
            .sasl(software.amazon.awssdk.services.kafka.model.ServerlessSasl.builder()
                .iam(software.amazon.awssdk.services.kafka.model.Iam.builder().enabled(true).build())
                .build())
            .build();

    protected static final List<software.amazon.awssdk.services.kafka.model.VpcConfig>
        VPC_CONFIG_LIST_RESPONSE = Lists.newArrayList(software.amazon.awssdk.services.kafka.model.VpcConfig.builder()
        .subnetIds(SUBNET_IDS).securityGroupIds(SECURITY_GROUP_IDS)
        .build());

    static {
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");
        logger = new LoggerProxy();
    }

    static ProxyClient<KafkaClient> MOCK_PROXY(
        final AmazonWebServicesClientProxy proxy,
        final KafkaClient kafkaClient) {
        return new ProxyClient<KafkaClient>() {
            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseT
            injectCredentialsAndInvokeV2(RequestT request, Function<RequestT, ResponseT> requestFunction) {
                return proxy.injectCredentialsAndInvokeV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            CompletableFuture<ResponseT>
            injectCredentialsAndInvokeV2Async(RequestT request,
                                              Function<RequestT, CompletableFuture<ResponseT>> requestFunction) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse, IterableT extends SdkIterable<ResponseT>>
            IterableT
            injectCredentialsAndInvokeIterableV2(RequestT request, Function<RequestT, IterableT> requestFunction) {
                return proxy.injectCredentialsAndInvokeIterableV2(request, requestFunction);
            }

            @Override
            public KafkaClient client() {
                return kafkaClient;
            }
        };
    }

    protected static ResourceModel buildResourceModel() {
        return ResourceModel.builder()
            .clusterName(CLUSTER_NAME)
            .arn(CLUSTER_ARN)
            .clientAuthentication(CLIENT_AUTHENTICATION)
            .vpcConfigs(Sets.newHashSet(VPC_CONFIG_LIST))
            .tags(TAGS)
            .build();
    }

    protected static ResourceModel buildResourceModelWithTags(Map<String, String> tags) {
        return ResourceModel.builder()
            .clusterName(CLUSTER_NAME)
            .arn(CLUSTER_ARN)
            .clientAuthentication(CLIENT_AUTHENTICATION)
            .vpcConfigs(Sets.newHashSet(VPC_CONFIG_LIST))
            .tags(tags)
            .build();
    }

    protected Cluster getServerlessCluster(ClusterState clusterState) {
        return Cluster.builder()
            .state(clusterState)
            .clusterName(CLUSTER_NAME)
            .clusterArn(CLUSTER_ARN)
            .clusterType(ClusterType.SERVERLESS)
            .serverless(Serverless.builder()
                .clientAuthentication(CLIENT_AUTHENTICATION_RESPONSE)
                .vpcConfigs(VPC_CONFIG_LIST_RESPONSE)
                .build()
            )
            .tags(TAGS)
            .build();
    }
}
