package com.atguigu.chapter06;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//统计热门url
public class UrlCountViewExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1).getConfig().setAutoWatermarkInterval(100);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource()). //乱序流水位线生成
                assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));
        stream.print("input");
//统计每个url的访问量
        stream.keyBy(data->data.url) //设置滚动窗口为10s
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UrlViewCountAgg(),new UrlViewCountResult())
                .print("output");
        env.execute();
    }
//    自定义聚合,来一条数据+1
    public static class UrlViewCountAgg implements AggregateFunction<Event,Long,Long>{

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
    // 自定义窗口处理函数，只需要包装窗口信息
public static class UrlViewCountResult extends ProcessWindowFunction<Long,UrlViewCount,String, TimeWindow>{
        @Override
        public void process(String s, Context context, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
//            迭代器中只有一个元素,就是增量计算的结果
            collector.collect(new UrlViewCount(s,iterable.iterator().next(),start,end));
        }
    }
}
