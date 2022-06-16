package software.amazon.msk.serverlesscluster;

import java.util.Map;

class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-msk-serverlesscluster.json");
    }

    /**
     * Providers should implement this method if their resource has a 'Tags' property to define resource-level tags
     * @return
     */
    public Map<String, String> resourceDefinedTags(final ResourceModel resourceModel) {
        if (resourceModel.getTags() == null) {
            return null;
        } else {
            return resourceModel.getTags();
        }
    }
}
