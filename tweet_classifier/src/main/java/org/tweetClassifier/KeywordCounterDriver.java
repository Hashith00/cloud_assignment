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
            System.err.println("The program will attempt to find a column named 'text_tweet' in the header.");
            System.err.println("It will count occurrences of predefined keywords: shooting, swimming, badminton, basketball, boxing, cycling, football.");
            System.err.println("Optionally, set CSV delimiter, e.g., -D csv.delimiter=\";\"");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Tweet Keyword Counter");
        job.setJarByClass(KeywordCounterDriver.class);

        job.setMapperClass(TweetKeywordMapper.class); // Use the new mapper
        job.setCombinerClass(KeywordCountReducer.class); // Combiner can be used
        job.setReducerClass(KeywordCountReducer.class); // Use the (renamed) reducer

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
