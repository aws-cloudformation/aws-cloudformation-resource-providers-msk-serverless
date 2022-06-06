# AWS::MSK::ServerlessCluster

Resource Type definition for AWS::MSK::ServerlessCluster

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::MSK::ServerlessCluster",
    "Properties" : {
        "<a href="#clustername" title="ClusterName">ClusterName</a>" : <i>String</i>,
        "<a href="#vpcconfigs" title="VpcConfigs">VpcConfigs</a>" : <i>[ <a href="vpcconfig.md">VpcConfig</a>, ... ]</i>,
        "<a href="#clientauthentication" title="ClientAuthentication">ClientAuthentication</a>" : <i><a href="serverlessclientauthentication.md">ServerlessClientAuthentication</a></i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i><a href="tags.md">Tags</a></i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::MSK::ServerlessCluster
Properties:
    <a href="#clustername" title="ClusterName">ClusterName</a>: <i>String</i>
    <a href="#vpcconfigs" title="VpcConfigs">VpcConfigs</a>: <i>
      - <a href="vpcconfig.md">VpcConfig</a></i>
    <a href="#clientauthentication" title="ClientAuthentication">ClientAuthentication</a>: <i><a href="serverlessclientauthentication.md">ServerlessClientAuthentication</a></i>
    <a href="#tags" title="Tags">Tags</a>: <i><a href="tags.md">Tags</a></i>
</pre>

## Properties

#### ClusterName

_Required_: Yes

_Type_: String

_Minimum_: <code>1</code>

_Maximum_: <code>64</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### VpcConfigs

_Required_: Yes

_Type_: List of <a href="vpcconfig.md">VpcConfig</a>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### ClientAuthentication

_Required_: Yes

_Type_: <a href="serverlessclientauthentication.md">ServerlessClientAuthentication</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Tags

A key-value pair to associate with a resource.

_Required_: No

_Type_: <a href="tags.md">Tags</a>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the Arn.

### Fn::GetAtt

The `Fn::GetAtt` intrinsic function returns a value for a specified attribute of this type. The following are the available attributes and sample return values.

For more information about using the `Fn::GetAtt` intrinsic function, see [Fn::GetAtt](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/intrinsic-function-reference-getatt.html).

#### Arn

Returns the <code>Arn</code> value.
