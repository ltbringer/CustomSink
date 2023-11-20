package spendreport;

import java.util.UUID;

public class FeatureBuilder {
    final private Feature feature;

    public FeatureBuilder() {
        this.feature = new Feature();
    }

    public FeatureBuilder id(long id) {
        this.feature.setId(id);
        return this;
    }

    public FeatureBuilder data(byte[] data) {
        this.feature.setData(data);
        return this;
    }

    public FeatureBuilder ts(long ts) {
        this.feature.setTs(ts);
        return this;
    }

    private FeatureBuilder key() {
        return this;
    }

    public Feature build() {
        this.feature.setKey(UUID.randomUUID());
        return this.feature;
    }

}