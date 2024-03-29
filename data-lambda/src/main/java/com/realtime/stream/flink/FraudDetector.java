package com.realtime.stream.flink;

import com.realtime.generator.SalesOrder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import scala.Tuple2;

public class FraudDetector extends KeyedProcessFunction<String, SalesOrder, Tuple2<String, String>> {

    private static final long serialVersionUID = 1L;


    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;


    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag", Types.BOOLEAN
        );
        flagState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer-state",
                Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(SalesOrder salesOrder, Context context,
                               Collector<Tuple2<String, String>> collector) throws Exception {

        Boolean lastTransactionWasSmall = flagState.value();
        if (lastTransactionWasSmall != null) {
            if (salesOrder.getActualGmv() > 5) {
                collector.collect(new Tuple2<>(salesOrder.getBuyerId(),
                        salesOrder.getSalesOrderId()));
            }
            flagState.clear();
        }

        if (salesOrder.getActualGmv() < 3) {
            flagState.update(true);

            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);
            timerState.update(timer);

        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, String>> out) {
        // remove flag after 1 minute
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        // delete timer
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // clean up all state
        timerState.clear();
        flagState.clear();
    }
}
