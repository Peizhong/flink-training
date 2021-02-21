package org.apache.flink.training.exercises.common.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.exercises.common.datatypes.MonitorEvent;

import java.time.Instant;
import java.util.Random;

public class MonitorGenerator implements SourceFunction<MonitorEvent> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<MonitorEvent> ctx) throws Exception {
        long id = 1;
        Random r = new Random(1);
        while (running) {
            MonitorEvent event = new MonitorEvent(id);
            event.env = "dev";
            if (id%3==1){
                event.env = "prod";
            }
            event.ruleId = (id % 10) + 1;
            event.eventTime = Instant.now();
            event.value = r.nextFloat() + 10;

            id += 1;

            ctx.collectWithTimestamp(event,event.getEventTime());
            ctx.emitWatermark(new Watermark(event.getEventTime()));

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
