package com.atguigu.chapter08;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 使用侧输出流
 */
public class SplitStreamByOutputTag {
    //    // 定义输出标签，侧输出流的数据类型为三元组(user, url, timestamp)
    private static OutputTag<Tuple3<String, String, Long>> MaryTag = new OutputTag<Tuple3<String, String, Long>>("Mary-pv") {
    };
    private static OutputTag<Tuple3<String, String, Long>>  BobTag= new OutputTag<Tuple3<String, String, Long>>("Bob-pv") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> processStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, Context context, Collector<Event> collector) throws Exception {
                if (event.user.equals("Mary")) {
                    context.output(MaryTag, new Tuple3<>(event.user, event.url, event.timestamp));
                } else if (event.user.equals("Bob")) {
                    context.output(BobTag, new Tuple3<>(event.user, event.url, event.timestamp));
                } else {
                    collector.collect(event);
                }
            }
        });
        processStream.getSideOutput(MaryTag).print();
        processStream.getSideOutput(BobTag).print();
        processStream.print("else");
        env.execute();

    }
}
