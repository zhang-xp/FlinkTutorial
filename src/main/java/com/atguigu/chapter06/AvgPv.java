package com.atguigu.chapter06;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashSet;

public class AvgPv implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {
    //    创建累加器 这就是为聚合创建了一个初始状态，每个聚 合任务只会调用一次。
    @Override
    public Tuple2<Long, HashSet<String>> createAccumulator() {
        return Tuple2.of(0L, new HashSet<String>());
    }

    // 属于本窗口的数据来一条累加一次，并返回累加器
    @Override
    public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> accumulator) {
//        每来一个数据都会调用add方法 ,pv+1,将user放入hashset
        accumulator.f1.add(event.user);
        return Tuple2.of(accumulator.f0 + 1, accumulator.f1);
    }
    // 窗口闭合时，增量聚合结束，将计算结果发送到下游

    @Override
    public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
        return (double) accumulator.f0 / accumulator.f1.size();
    }

    @Override
    public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
        return null;
    }


}
