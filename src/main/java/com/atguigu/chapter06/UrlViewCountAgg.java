package com.atguigu.chapter06;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.functions.AggregateFunction;

public class UrlViewCountAgg  implements AggregateFunction<Event,Long,Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Event event, Long accumulator) {

        return accumulator+1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return null;
    }
}
