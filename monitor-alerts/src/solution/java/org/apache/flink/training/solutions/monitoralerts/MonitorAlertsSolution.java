package org.apache.flink.training.solutions.monitoralerts;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.experimental.CollectSink;
import org.apache.flink.training.exercises.common.datatypes.MonitorAlert;
import org.apache.flink.training.exercises.common.datatypes.MonitorEvent;
import org.apache.flink.training.exercises.common.sources.MonitorGenerator;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.util.Collection;
import java.util.function.Consumer;

public class MonitorAlertsSolution {
    public static void main(String[] args) throws Exception {
        Boolean active = true;
        Boolean inactive = false;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<MonitorEvent> events = env.addSource(new MonitorGenerator());

        if (inactive){
            events.print();
        }

        if (inactive){
            DataStream<MonitorEvent> prodEvents = events.keyBy(e->e.env).filter(new FilterFunction<MonitorEvent>() {
                @Override
                public boolean filter(MonitorEvent value) throws Exception {
                    return value.env.equals("prod");
                }
            });
            prodEvents.print();
        }

        if (active) {

            WindowedStream<MonitorEvent, Long, TimeWindow> alerts = events.keyBy(e -> e.ruleId).window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)));

            DataStream<Tuple3<String, Long, Long>> count = alerts.process(new ProcessWindowFunction<MonitorEvent, Tuple3<String, Long, Long>, Long, TimeWindow>() {
                @Override
                public void process(Long aLong, Context context, Iterable<MonitorEvent> elements, Collector<Tuple3<String, Long, Long>> out) throws Exception {
                    out.collect(Tuple3.of("dev", aLong, 1L));
                }
            });
            count.print();
        }

        env.execute("Monitor Alerts");
    }
}
