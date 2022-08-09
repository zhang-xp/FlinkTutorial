package com.atguigu.chapter08;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 多流转换
 */
public class SplitStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> maryStream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("Mary");
            }
        });
        SingleOutputStreamOperator<Event> bobStream = stream.filter(
                new FilterFunction<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.user.equals("Bob");
                    }
                }
        );
        SingleOutputStreamOperator<Event> elseStream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return !event.user.equals("Mary") && !event.user.equals("Bob");
            }
        });
        maryStream.print("mary pv");
        bobStream.print("bob pv");
        elseStream.print("else pv");
        env.execute();
    }
}
