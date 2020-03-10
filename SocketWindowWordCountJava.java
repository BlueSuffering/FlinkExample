package flink;
/*
 *@description
 *@author:yanlinshan
 *@date: -
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Random;

public class SocketWindowWordCountJava {
    public static void main(String[] args) throws Exception {
        // 获取所需要的端口号
        int port = 39001;
//        try{
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        port = parameterTool.getInt("port");}
//        catch (Exception e){
//            System.err.println("no port specified. use default 9000");
//            port = 9000;
//        }
        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "127.0.0.1";
        String delimiter = "\n";
        // 链接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);
        DataStream<String> windowCounts = text.flatMap(new FlatMapFunction<String, WordIsCount>() {
            @Override
            public void flatMap(String value, Collector<WordIsCount> out) throws Exception {
                String[] words = value.split("\\s");
                for (String word : words) {
                    out.collect(new WordIsCount(word, 1L));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(6), Time.seconds(3))// 指定时间窗口大小为2秒，指定时间间隔为1秒
                .sum("count")
                .map(new MapFunction<WordIsCount, String>() {
                    @Override
                    public String map(WordIsCount value) throws Exception {

                        return "------------------------------\n字符是：\t"+value.word + "统计的结果是：\t"+value.word+"\n" +
                                "===============================";
                    }
                });// 在这里使用sum或者reduce都可以
        // 将数据打印到控制台，并设置并行度
        windowCounts.print().setParallelism(1);

        // 这一行代码一定要实现，否则不执行
        env.execute("socket window count");

    }

    public static class WordIsCount {
        public String word;
        public long count;

        public WordIsCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public WordIsCount() {
        }

        @Override
        public String toString() {
            return "WordIsCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}