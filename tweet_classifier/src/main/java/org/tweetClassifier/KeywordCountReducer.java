package org.tweetClassifier;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KeywordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable totalCount = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        totalCount.set(sum);
        context.write(key, totalCount); // Outputs: <keyword>  <total_count_for_that_keyword>
    }
}
