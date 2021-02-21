package org.apache.flink.training.exercises.common.datatypes;

import java.io.Serializable;
import java.time.Instant;

public class MonitorEvent implements Serializable {

    public MonitorEvent() {
        this.eventTime = Instant.now();
    }

    public MonitorEvent(long eventId) {
        this.eventId = eventId;
    }

    public MonitorEvent(long eventId,long ruleId,Instant eventTime,String env,float value){
        this.eventId = eventId;
        this.ruleId = ruleId;
        this.eventTime = eventTime;
        this.env = env;
        this.value = value;
    }

    public long eventId;
    public long ruleId;
    public Instant eventTime;
    public String env;
    public float value;

    @Override
    public String toString() {
        return eventId+","+ruleId+","+env;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof MonitorEvent &&
                this.eventId == ((MonitorEvent)obj).eventId;
    }

    @Override
    public int hashCode() {
        return (int)this.eventId;
    }

    public long getEventTime() {
        return this.eventTime.toEpochMilli();
    }
}
