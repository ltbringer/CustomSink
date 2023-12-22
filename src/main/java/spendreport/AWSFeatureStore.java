package spendreport;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sagemaker.model.FeatureDefinition;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.SageMakerFeatureStoreRuntimeClient;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AWSFeatureStore implements SinkWriter<Row> {

    private final String featureGroupName;
    private final TtlDuration ttlDuration;

    private final SageMakerFeatureStoreRuntimeClient client;
    private final Deque<Collection<FeatureValue>> features = new ArrayDeque<>();
    private final List<FeatureDefinition> featureDefinitions;
    private final ExecutorService executorService;
    private final Object lockFeatures = new Object();


    public AWSFeatureStore(String featureGroupName, TtlDuration ttl, List<FeatureDefinition> featureDefinitions, int nThreads) {
        this.featureGroupName = featureGroupName;
        this.ttlDuration = ttl;
        this.featureDefinitions = featureDefinitions;
        this.executorService = Executors.newFixedThreadPool(nThreads);

        client = SageMakerFeatureStoreRuntimeClient
                .builder()
                .region(Region.EU_CENTRAL_1)
                .build();
    }

    private FeatureValue createFeatureValue(String key, String value) {
        return FeatureValue.builder()
                .featureName(key)
                .valueAsString(value)
                .build();
    }

    @Override
    public void write(Row value, Context context) throws IOException, InterruptedException {
        Collection<FeatureValue> featureValues = new ArrayList<>();
        for (FeatureDefinition featureDefinition : featureDefinitions) {
            String featureName = featureDefinition.featureName();
            String featureType = String.valueOf(featureDefinition.featureType());
            Optional<String> stringValue = Optional.empty();

            switch (featureType) {
                case "String":
                    stringValue = Optional.ofNullable((String) value.getField(featureName));
                    break;

                case "Integral":
                    stringValue = Optional.ofNullable((Long) value.getField(featureName)).map(String::valueOf);
                    break;

                case "Fractional":
                    stringValue = Optional.ofNullable((Double) value.getField(featureName)).map(String::valueOf);
                    break;

                default:
                    Log.error("Unsupported feature type: " + featureType);
            }

            stringValue.ifPresent(s -> featureValues.add(createFeatureValue(featureName, s)));
            Log.trace("Feature: [" + featureType + "] " + featureName + ": " + stringValue.orElse("null"));
        }

        synchronized (lockFeatures) {
            if (featureValues.size() == featureDefinitions.size()) {
                features.push(featureValues);
            } else {
                Log.warn("Skipping feature due to missing values in: " + featureValues);
            }
        }
    }

    private void commit(Collection<FeatureValue> feature) throws IOException {
        PutRecordRequest request = PutRecordRequest.builder()
                .featureGroupName(featureGroupName)
                .ttlDuration(ttlDuration)
                .record(feature)
                .build();

        Log.debug("Sending request to AWS: " + request.toString());

        PutRecordResponse res = client.putRecord(request);

        Log.debug("[" + res.sdkHttpResponse().statusCode() + "] " + res.sdkHttpResponse().statusText());

        if (!res.sdkHttpResponse().isSuccessful()) {
            throw new IOException("Failed to put record");
        }
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {
        synchronized (lockFeatures) {
            while (!features.isEmpty()) {
                Collection<FeatureValue> feature = features.pop();
                executorService.submit(() -> {
                    try {
                        commit(feature);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    @Override
    public void close() throws Exception {
        client.close();
        executorService.shutdown();
        try {
            boolean result = executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            if (!result) {
                throw new IOException("Failed to flush all records");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
