package com.atguigu.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformKeyByTest {
    public static void main(String[] args) throws Exception {
        //       创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3000L),
                new Event("Bob", "./home", 4000L),
                new Event("Bob", "./prod?id=2", 5000L),
                new Event("Bob", "./prod?id=3", 6000L)
        );
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp").print("max: ");
        stream.keyBy(data->data.user).maxBy("timestamp").print("maxby: ");
        env.execute();
    }
}
