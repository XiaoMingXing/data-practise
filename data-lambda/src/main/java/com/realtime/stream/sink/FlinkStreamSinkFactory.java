package com.realtime.stream.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class FlinkStreamSinkFactory {
    public static SinkFunction<String> getDefaultFileSink() {

        return StreamingFileSink.forRowFormat(new Path("sink/test"),
                new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.create()
                        .withRolloverInterval(TimeUnit.SECONDS.toMillis(60))
                        .withInactivityInterval(TimeUnit.SECONDS.toMillis(10))
                        .withMaxPartSize(1024)
                        .build()
                ).build();
    }
}
