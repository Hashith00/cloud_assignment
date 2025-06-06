package org.tweetClassifier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KeywordCounterDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: KeywordCounterDriver [genericOptions] <input_path> <output_path>");
            System.err.println("You MUST provide the column index via command line: -D csv.text.column.index=<index>");
            System.exit(2);
        }

        // Re-add the check to ensure the user provides the index
        if (conf.get("csv.text.column.index") == null) {
            System.err.println("Error: The property 'csv.text.column.index' must be set.");
            System.err.println("Provide it using -D csv.text.column.index=<your_column_index>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "Tweet Keyword Counter");
        job.setJarByClass(KeywordCounterDriver.class);

        job.setMapperClass(TweetKeywordMapper.class);
        job.setCombinerClass(KeywordCountReducer.class);
        job.setReducerClass(KeywordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
