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

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.time.Instant;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class DataGenerator extends KeyedProcessFunction<Long, Transaction, Row> {

	private static final long serialVersionUID = 1L;

	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Row> collector) {

		Row feature = Row.withNames();
		feature.setField("id", transaction.getAccountId());
		feature.setField("data", Double.valueOf(transaction.getAmount()).longValue());
		feature.setField("ts", Instant.now().toEpochMilli() / 1000.0);
		collector.collect(feature);
	}
}
