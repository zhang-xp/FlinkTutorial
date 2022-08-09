package com.atguigu.chapter08;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 连接（Connect）
 * ConnectedStreams  两条流数据类型可以不同,只是放在同一个流中
 * 两条流的连接（connect），与联合（union）操作相比，最大的优势就是可以处理不同类型的流的合并，
 * 使用更灵活、应用更广泛。当然它也有限制，就是合并流的数量只能是 2，而 union 可以同时进行多条流的合并
 */
public class CoMapExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long> stream2 = env.fromElements(1L, 2L, 3L);
        ConnectedStreams<Integer, Long> connectStreams = stream1.connect(stream2);
        SingleOutputStreamOperator<String> result = connectStreams.map(new CoMapFunction<Integer, Long, String>() {

            @Override
            public String map1(Integer integer) throws Exception {
                return "Integer" + integer;
            }

            @Override
            public String map2(Long aLong) throws Exception {
                return "Long" + aLong;
            }
        });
        result.print();
        env.execute();

    }
}
