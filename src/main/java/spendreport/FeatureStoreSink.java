package spendreport;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;

public class FeatureStoreSink implements Sink<Row> {

    @Override
    public SinkWriter<Row> createWriter(InitContext initContext) throws IOException {
        return new FeatureStore("features", "features.db");
    }
}
