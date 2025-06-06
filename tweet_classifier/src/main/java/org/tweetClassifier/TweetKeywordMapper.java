package org.tweetClassifier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TweetKeywordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text keywordOutputKey = new Text();

    private int textColumnIndex; // Index for the 'text_tweet' column
    private String csvDelimiter;
    private List<String> targetKeywords;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        csvDelimiter = conf.get("csv.delimiter", ",");

        // IMPORTANT: Read the column index from the configuration
        textColumnIndex = conf.getInt("csv.text.column.index", -1);
        if (textColumnIndex < 0) {
            throw new IllegalArgumentException(
                    "The column index ('csv.text.column.index') must be set via -D csv.text.column.index=<index>");
        }

        // Define the list of keywords to search for
        targetKeywords = new ArrayList<>(Arrays.asList(
                "shooting", "swimming", "badminton", "basketball", "boxing", "cycling", "football"
        ));
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // You might want to skip the header row manually. This checks if it's the first line of a file.
        if (key.get() == 0) {
            // Add a check to see if this line looks like a header, and if so, skip it.
            // For now, we will assume all lines are data and rely on the user to handle the header.
            // A more robust solution would be to filter it out before the job.
        }

        if (line == null || line.trim().isEmpty()) {
            return;
        }

        String[] parts = line.split(csvDelimiter);

        if (parts.length > textColumnIndex) {
            String tweetTextLowercase = parts[textColumnIndex].trim().toLowerCase();
            if (tweetTextLowercase.isEmpty()) {
                return;
            }

            for (String keyword : targetKeywords) {
                if (tweetTextLowercase.contains(keyword)) {
                    keywordOutputKey.set(keyword);
                    context.write(keywordOutputKey, one);
                }
            }
        }
    }
}
