package software.amazon.msk.serverlesscluster;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;

import software.amazon.awssdk.services.kafka.model.Cluster;
import software.amazon.awssdk.services.kafka.model.ClusterType;
import software.amazon.awssdk.services.kafka.model.CreateClusterV2Request;
import software.amazon.awssdk.services.kafka.model.DeleteClusterRequest;
import software.amazon.awssdk.services.kafka.model.DescribeClusterV2Request;
import software.amazon.awssdk.services.kafka.model.DescribeClusterV2Response;
import software.amazon.awssdk.services.kafka.model.Iam;
import software.amazon.awssdk.services.kafka.model.ListClustersV2Request;
import software.amazon.awssdk.services.kafka.model.ListClustersV2Response;
import software.amazon.awssdk.services.kafka.model.ServerlessClientAuthentication;
import software.amazon.awssdk.services.kafka.model.ServerlessRequest;
import software.amazon.awssdk.services.kafka.model.ServerlessSasl;
import software.amazon.awssdk.services.kafka.model.VpcConfig;

/**
 * This class is a centralized placeholder for
 * - api request construction
 * - object translation to/from aws sdk
 * - resource model construction for read/list handlers
 */

public class Translator {

    /**
     * Request to create a resource
     *
     * @param model resource model
     * @return CreateClusterRequest the aws service request to create a resource
     */
    static CreateClusterV2Request translateToCreateRequest(final ResourceModel model) {
        return CreateClusterV2Request.builder()
            .clusterName(model.getClusterName())
            .serverless(
                ServerlessRequest.builder()
                    .clientAuthentication(ServerlessClientAuthentication.builder()
                        .sasl(ServerlessSasl.builder()
                            .iam(Iam.builder()
                                .enabled(model.getClientAuthentication().getSasl().getIam().getEnabled())
                                .build())
                            .build())
                        .build())
                    .vpcConfigs(model.getVpcConfigs().stream().map(vpcConfig -> VpcConfig.builder()
                        .securityGroupIds(vpcConfig.getSecurityGroups())
                        .subnetIds(vpcConfig.getSubnetIds())
                        .build()).collect(Collectors.toList()))
                    .build()
            )
            .tags(model.getTags())
            .build();
    }

    /**
     * Request to read a resource
     *
     * @param model resource model
     * @return DescribeClusterRequest the aws service request to describe a resource
     */
    static DescribeClusterV2Request translateToReadRequest(final ResourceModel model) {
        return DescribeClusterV2Request.builder().clusterArn(model.getArn()).build();
    }

    /**
     * Translates resource object from sdk into a resource model
     *
     * @param describeClusterResponse the aws service describe resource response
     * @return model resource model
     */
    static ResourceModel translateFromReadResponse(final DescribeClusterV2Response describeClusterResponse) {
        ResourceModel resourceModel = ResourceModel.builder()
            .arn(describeClusterResponse.clusterInfo().clusterArn())
            .clusterName(describeClusterResponse.clusterInfo().clusterName())
            .clientAuthentication(software.amazon.msk.serverlesscluster.ClientAuthentication.builder()
                .sasl(software.amazon.msk.serverlesscluster.Sasl.builder()
                    .iam(software.amazon.msk.serverlesscluster.Iam.builder()
                        .enabled(describeClusterResponse.clusterInfo().serverless().clientAuthentication().sasl().iam()
                            .enabled())
                        .build())
                    .build())
                .build())
            .vpcConfigs(describeClusterResponse.clusterInfo().serverless().vpcConfigs().stream().map(vpcConfig ->
                software.amazon.msk.serverlesscluster.VpcConfig.builder()
                    .securityGroups(Sets.newHashSet(vpcConfig.securityGroupIds()))
                    .subnetIds(Sets.newHashSet(vpcConfig.subnetIds()))
                    .build()).collect(Collectors.toSet()))
            .tags(describeClusterResponse.clusterInfo().tags())
            .build();
        return resourceModel;
    }

    /**
     * Request to delete a resource
     *
     * @param model resource model
     * @return DeleteClusterRequest the aws service request to delete a resource
     */
    static DeleteClusterRequest translateToDeleteRequest(final ResourceModel model) {
        return DeleteClusterRequest.builder().clusterArn(model.getArn()).build();
    }

    /**
     * Request to list resources (Serverless clusters only) within aws account
     *
     * @param nextToken token passed to the aws service describe resource request
     * @return listClustersRequest the aws service request to describe resources within aws account
     */
    static ListClustersV2Request translateToListRequest(final String nextToken) {
        return ListClustersV2Request.builder()
            .clusterTypeFilter(ClusterType.SERVERLESS.name())
            .nextToken(nextToken)
            .build();
    }

    /**
     * Translates resource objects from sdk into a resource model (primary identifier only)
     *
     * @param listClustersResponse the aws service describe resource response
     * @return list of resource models
     */
    static List<ResourceModel> translateFromListResponse(final ListClustersV2Response listClustersResponse) {
        final List<Cluster> clustersList = listClustersResponse.clusterInfoList();
        return streamOfOrEmpty(clustersList)
            .map(cluster -> ResourceModel.builder().arn(cluster.clusterArn()).build())
            .collect(Collectors.toList());
    }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
            .map(Collection::stream)
            .orElseGet(Stream::empty);
    }
}
