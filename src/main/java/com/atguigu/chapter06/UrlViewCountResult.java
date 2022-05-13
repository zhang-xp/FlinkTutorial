package com.atguigu.chapter06;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UrlViewCountResult extends ProcessWindowFunction<Long,UrlViewCount,String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
        long start = context.window().getStart();
        long end = context.window().getEnd();
//            迭代器中只有一个元素,就是增量计算的结果
        collector.collect(new UrlViewCount(s,iterable.iterator().next(),start,end));
    }

}
