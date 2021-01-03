package BatchJob;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputpath = "src/main/resources/WordInput";
        DataSource<String> source = env.readTextFile(inputpath);
        AggregateOperator<Tuple2<String, Integer>> sum = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words
                ) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).groupBy(0).sum(1);
        sum.print();

    }
}
