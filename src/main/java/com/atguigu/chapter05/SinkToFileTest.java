package com.atguigu.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.sql.Time;
import java.util.concurrent.TimeUnit;
//向文件中写数据
public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        //       创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Alice", "./prod?id=100", 3200L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
        );
        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(
                new Path("./output"),
                new SimpleStringEncoder<>("utf-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                                .withInactivityInterval(TimeUnit.MINUTES.toMinutes(15))
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024).build()).build();
        // 将 Event 转换成 String 写入文件
        stream.map(Event::toString).addSink(fileSink);

        env.execute();
    }
}
