package org.simpleWordCount;

// AttackFrequencyDriver.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AttackFrequencyDriver {

    // Configuration keys for the column names
    public static final String ATTACK_TYPE_HEADER_CONF = "mapreduce.task.header.attacktype";
    public static final String SEVERITY_LEVEL_HEADER_CONF = "mapreduce.task.header.severitylevel";
    public static final String ACTION_TAKEN_HEADER_CONF = "mapreduce.task.header.actiontaken";

    // Configuration keys for the filter values
    public static final String FILTER_SEVERITY_VALUE_CONF = "mapreduce.task.filter.severityvalue";
    public static final String FILTER_ACTION_VALUE_CONF = "mapreduce.task.filter.actionvalue";


    public static void main(String[] args) throws Exception {
        if (args.length != 2) { // Simplified for this example; add more args if making headers/filters dynamic
            System.err.println("Usage: AttackFrequencyDriver <input path> <output path>");
            // For a production job, you might pass header names and filter values as additional arguments
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // Set the target header names and filter values
        // These can be hardcoded here, or parsed from command-line args for more flexibility
        conf.set(ATTACK_TYPE_HEADER_CONF, "Attack Type");
        conf.set(SEVERITY_LEVEL_HEADER_CONF, "Severity Level");
        conf.set(ACTION_TAKEN_HEADER_CONF, "Action Taken");

        conf.set(FILTER_SEVERITY_VALUE_CONF, "High");
        conf.set(FILTER_ACTION_VALUE_CONF, "ignored");

        Job job = Job.getInstance(conf, "Filtered Attack Type Count");

        job.setJarByClass(AttackFrequencyDriver.class);
        job.setMapperClass(AttackTypeMapper.class);
        job.setCombinerClass(AttackTypeReducer.class); // Assuming reducer is still a sum
        job.setReducerClass(AttackTypeReducer.class);   // Assuming reducer is still a sum

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}