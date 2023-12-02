package spendreport;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

public class FeatureStoreSink implements Sink<Feature> {

    @Override
    public SinkWriter<Feature> createWriter(InitContext initContext) throws IOException {
        return new FeatureStore("features", "features.db");
    }
}
