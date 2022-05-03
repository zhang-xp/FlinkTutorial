package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformParitionTest {

    public static void main(String[] args) throws Exception {
        //    物理分区
//    创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
//    重缩放分区
//        env.addSource(new RichParallelSourceFunction<Integer>() {
//        @Override
//        public void run(SourceContext<Integer> sourceContext) throws Exception {
//            for (int i = 1; i <= 8; i++) {
//                if (i%2== getRuntimeContext().getIndexOfThisSubtask()){
////                    将奇偶数分别放入0和1分区
//                    sourceContext.collect(i);
//                }
//            }
//        }
//        @Override
//        public void cancel() {
//
//        }
//    }).setParallelism(2).rescale().print().setParallelism(4);

//    广播分区
//    stream.broadcast().print().setParallelism(4);
//    自定义分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8).
                partitionCustom(new Partitioner<Integer>() {
                                    @Override
                                    public int partition(Integer key, int i) {
                                        return key % 2;
                                    }
                                }, new KeySelector<Integer, Integer>() {
                                    @Override
                                    public Integer getKey(Integer value) throws Exception {
                                        return value;
                                    }
                                }
                ).print().setParallelism(4);
        env.execute();
    }
}
