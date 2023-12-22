package spendreport;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;
import software.amazon.awssdk.services.sagemaker.model.FeatureDefinition;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.TtlDuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AWSFeatureStoreSink implements Sink<Row> {
    private final String featureGroupName;
    private final TtlDuration ttlDuration;
    private final List<FeatureDefinition> featureDefinitions;
    private final int nThreads;

    public AWSFeatureStoreSink(String featureGroupName, String ttl, List<FeatureDefinition> featureDefinitions, int nThreads) {
        this.featureGroupName = featureGroupName;
        final String ttlUnit = ttl.substring(ttl.length() - 1);
        final int ttlValue = Integer.parseInt(ttl.substring(0, ttl.length() - 1));
        this.featureDefinitions = featureDefinitions;
        if (nThreads < 1) {
            nThreads = 1;
        }
        this.nThreads = nThreads;

        // 1s 1m 1h 1d 1w 1M 1y
        Map<String, String> unitMap = Map.of(
                "s", "Seconds",
                "m", "Minutes",
                "h", "Hours",
                "d", "Days",
                "w", "Weeks",
                "M", "Months",
                "y", "Years"
        );
        ttlDuration = TtlDuration.builder()
                .unit(unitMap.get(ttlUnit))
                .value(ttlValue)
                .build();
    }

    public static class Builder {
        private String featureGroupName;
        private String ttl;
        private List<FeatureDefinition> featureDefinitions;
        private int nThreads;


        public Builder withFeatureGroupName(String featureGroupName) {
            this.featureGroupName = featureGroupName;
            return this;
        }

        public Builder withTtl(String ttl) {
            this.ttl = ttl;
            return this;
        }

        public Builder withFeatureDefinitions(List<FeatureDefinition> featureDefinitions) {
            this.featureDefinitions = featureDefinitions;
            return this;
        }

        public Builder withMaxConcurrency(int maxConcurrency) {
            this.nThreads = maxConcurrency;
            return this;
        }

        public AWSFeatureStoreSink build() {
            return new AWSFeatureStoreSink(featureGroupName, ttl, featureDefinitions, nThreads);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public SinkWriter<Row> createWriter(InitContext initContext) throws IOException {
        return new AWSFeatureStore(featureGroupName, ttlDuration, featureDefinitions, nThreads);
    }
}
