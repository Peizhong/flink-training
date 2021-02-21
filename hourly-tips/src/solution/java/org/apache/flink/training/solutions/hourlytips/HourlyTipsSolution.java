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

package org.apache.flink.training.solutions.hourlytips;

import akka.stream.impl.fusing.Log;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the "Hourly Tips" exercise of the Flink training in the docs.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 */
public class HourlyTipsSolution extends ExerciseBase {

	/**
	 * Main method.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public static void main(String[] args) throws Exception {

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareGenerator())); // 这个有水位线

		// compute tips per hour for each driver
		var hourlyTipsByDriver = fares.keyBy((TaxiFare fare) -> fare.driverId);

		var hourlyTipsSum = hourlyTipsByDriver.window(TumblingEventTimeWindows.of(Time.hours(1))).aggregate(new AggregateFunction<TaxiFare, Tuple2<Long,Float>, Tuple2<Long,Float>>() {
			@Override
			public Tuple2<Long, Float> createAccumulator() {
				return Tuple2.of(0L,0F);
			}

			@Override
			public Tuple2<Long, Float> add(TaxiFare value, Tuple2<Long, Float> accumulator) {
				return Tuple2.of(value.rideId,accumulator.f0+value.tip);
			}

			@Override
			public Tuple2<Long, Float> getResult(Tuple2<Long, Float> accumulator) {
				return Tuple2.of(accumulator.f0,accumulator.f1);
			}

			@Override
			public Tuple2<Long, Float> merge(Tuple2<Long, Float> a, Tuple2<Long, Float> b) {
				return Tuple2.of(a.f0,a.f1+b.f1);
			}
		});

		var hourlySum = hourlyTipsByDriver.window(TumblingEventTimeWindows.of(Time.hours(1))).sum(1);

		var hourlyTips = hourlyTipsByDriver.window(TumblingEventTimeWindows.of(Time.hours(1))) // 滚动窗口
				.process(new AddTips());

		DataStream<Tuple2<Long,Long>> driverAndWindow = hourlyTips.process(new ProcessFunction<Tuple3<Long, Long, Float>, Tuple2<Long, Long>>() {
			@Override
			public void processElement(Tuple3<Long, Long, Float> value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
				// driverId & windowTime
				out.collect(Tuple2.of(value.f1, value.f0));
			}
		});


		DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
				.windowAll(TumblingEventTimeWindows.of(Time.hours(1))) // 并行度变为1
				.maxBy(2);

//		You should explore how this alternative behaves. In what ways is the same as,
//		and different from, the solution above (using a windowAll)?

// 		DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
// 			.keyBy(t -> t.f0) // 第一个值是窗口结束时间，第二个值是司机id，第三个值是总额。// todo: why
// 			.maxBy(2);

		printOrTest(driverAndWindow);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

	/*
	 * Wraps the pre-aggregated result into a tuple along with the window's timestamp and key.
	 */
	public static class AddTips extends ProcessWindowFunction<
			TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

		@Override
		public void process(Long key, Context context, Iterable<TaxiFare> fares, Collector<Tuple3<Long, Long, Float>> out) {
			float sumOfTips = 0F;
			for (TaxiFare f : fares) {
				sumOfTips += f.tip;
			}
			out.collect(Tuple3.of(context.window().getEnd(), key, sumOfTips));
		}
	}
}
