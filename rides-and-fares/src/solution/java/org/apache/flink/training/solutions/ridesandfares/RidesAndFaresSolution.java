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

package org.apache.flink.training.solutions.ridesandfares;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * Java reference implementation for the "Stateful Enrichment" exercise of the Flink training in the docs.
 *
 * <p>The goal for this exercise is to enrich TaxiRides with fare information.
 *
 */
public class RidesAndFaresSolution extends ExerciseBase {

	/**
	 * Main method.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public static void main(String[] args) throws Exception {

		// Set up streaming execution environment, including Web UI and REST endpoint.
		// Checkpointing isn't needed for the RidesAndFares exercise; this setup is for
		// using the State Processor API.

		Configuration conf = new Configuration();
		conf.setString("state.backend", "filesystem");
		conf.setString("state.savepoints.dir", "file:///tmp/savepoints");
		conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		env.setParallelism(ExerciseBase.parallelism);

		env.enableCheckpointing(10000L);
		CheckpointConfig config = env.getCheckpointConfig();
		config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideGenerator()))
				.filter((TaxiRide ride) -> ride.isStart)
				.keyBy((TaxiRide ride) -> ride.rideId);

		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareGenerator()))
				.keyBy((TaxiFare fare) -> fare.rideId);

		// Set a UID on the stateful flatmap operator so we can read its state using the State Processor API.
		DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
				.connect(fares) // connect的2个流有相同的key
				.flatMap(new EnrichmentFunction())
				.uid("enrichment");

		DataStream<Tuple2<Long,Float>> result = enrichedRides.process(new ProcessFunction<Tuple2<TaxiRide, TaxiFare>, Tuple2<Long, Float>>() {
			@Override
			public void processElement(Tuple2<TaxiRide, TaxiFare> value, Context ctx, Collector<Tuple2<Long, Float>> out) throws Exception {
				out.collect(Tuple2.of(value.f0.rideId,value.f1.totalFare));
			}

			@Override
			public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, Float>> out) throws Exception {
				// ctx.timerService().currentProcessingTime()
			}
		});


		printOrTest(result);

		env.execute("Join Rides with Fares (java RichCoFlatMap)");
	}

	public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
		// keyed, managed state，每个key对应的状态
		private ValueState<TaxiRide> rideState;
		private ValueState<TaxiFare> fareState;

		@Override
		public void open(Configuration config) {
			rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
			fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
		}

		@Override
		public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiFare fare = fareState.value();
			if (fare != null) { // fare已经到达
				fareState.clear(); // ?用完了怎么回收
				out.collect(Tuple2.of(ride, fare));
			} else {
				rideState.update(ride);
			}
		}

		@Override
		public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiRide ride = rideState.value();
			if (ride != null) {
				rideState.clear();
				out.collect(Tuple2.of(ride, fare));
			} else {
				fareState.update(fare);
			}
		}
	}
}
