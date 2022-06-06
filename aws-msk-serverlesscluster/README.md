# AWS::MSK::ServerlessCluster

This resource enables the creation of AWS MSK Serverless Cluster via Cloudformation. In effect, you will now have the
resource, AWS::MSK::ServerlessCluster available to you to deploy using Cloudformation.

This has been made possible using Cloudformation Registry. For more details about Cloudformation Registry - please look at: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/registry.html

## Pre-Requisites
1. Java Version 8 or higher. Make sure you are using Java 8 by running `java -version`.
2. Apache Maven - `brew install maven` - if you are on a Mac
3. Cloudformation CLI and Cloudformation Java Plugin - https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/what-is-cloudformation-cli.html
4. Install [SAM](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html).

## Development and Local Testing
1. For updating the resource contract update aws-msk-serverlesscluster.json and run cfn generate.
2. Modify the appropriate handler.
3. Run `mvn clean install`
4. Create test files like
```
{
  "desiredResourceState": {
    "ClusterName": "testClusterName",
    "VpcConfigs": [{
      "SecurityGroups": ["sg-1234567890123"],
      "SubnetIds": ["subnet-1","subnet-2","subnet-3"]
    }],
    "ClientAuthentication": {
      "Sasl": {
        "Iam": {
          "Enabled": true
        }
      }
    },
    "Tags": {
      "tagKeyA": "valueA",
      "tagKeyB": "valueB"
    }
  },
  "previousResourceState": {},
  "logicalResourceIdentifier": "MyResource"
}
```
5. Run the command, `cfn invoke -v resource <action> <request>` to test the respective handler. For example, run `cfn invoke -v resource CREATE <create.json>` to test the CREATE handler.
