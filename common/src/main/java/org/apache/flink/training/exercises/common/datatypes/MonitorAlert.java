package org.apache.flink.training.exercises.common.datatypes;

import java.time.Instant;

public class MonitorAlert {
    public MonitorAlert(){
        this.startTime = Instant.now();
    }

    public MonitorAlert(long alertId,Instant startTime, String message){
        this.alertId = alertId;
        this.startTime = startTime;
        this.message = message;
    }

    public long alertId;
    public Instant startTime;
    public String message;
}
