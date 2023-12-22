/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;


import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;
import software.amazon.awssdk.services.sagemaker.model.FeatureDefinition;

import java.util.ArrayList;
import java.util.Map;

/**
 * Skeleton code for the datastream walkthrough
 */
public class AWSFeatureDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource())
                .name("transactions");

        DataStream<Row> features = transactions
                .keyBy(Transaction::getAccountId)
                .process(new DataGenerator())
                .name("fraud-detector");

        ArrayList<FeatureDefinition> featureDefinitions = new ArrayList<>();
        Map<String, String> featureTypeMap = Map.of(
                "id", "Integral",
                "data", "Integral",
                "ts", "Fractional"
        );
        for (Map.Entry<String, String> entry : featureTypeMap.entrySet()) {
            featureDefinitions.add(FeatureDefinition.builder()
                    .featureName(entry.getKey())
                    .featureType(entry.getValue())
                    .build());
        }

        AWSFeatureStoreSink featureStoreSink = AWSFeatureStoreSink.builder()
            .withFeatureGroupName("ml-platform-dev-test-feature-group")
            .withTtl("6h")
            .withFeatureDefinitions(featureDefinitions)
            .withMaxConcurrency(20)
            .build();

        features
                .sinkTo(featureStoreSink)
                .name("send-alerts");

        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.execute("Fraud Detection");
    }
}
