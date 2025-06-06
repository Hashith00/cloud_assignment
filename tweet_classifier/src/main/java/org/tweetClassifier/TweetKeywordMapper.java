package org.tweetClassifier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class TweetKeywordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text keywordOutputKey = new Text(); // To reuse Text object

    private int textColumnIndex = -1;
    private String targetColumnName = "text_tweet";
    private String csvDelimiter = ",";
    private boolean headerProcessedByThisMapper = false;

    private List<String> targetKeywords;

    // Enum for custom counters
    private enum MAPPER_COUNTERS {
        HEADER_NOT_FOUND,
        LINES_SKIPPED_NO_HEADER,
        COLUMN_NOT_FOUND_IN_HEADER,
        INVALID_ROW_SHORT
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        csvDelimiter = conf.get("csv.delimiter", ",");
        // targetColumnName = conf.get("target.column.name", "text_tweet"); // Could be configurable

        // Define the list of keywords to search for (converted to lowercase for case-insensitive matching)
        targetKeywords = new ArrayList<>(Arrays.asList(
                "shooting", "swimming", "badminton", "basketball", "boxing", "cycling", "football"
        ));
        // You could also make this list configurable via the Configuration object if needed,
        // e.g., by passing a comma-separated string:
        // String keywordsStr = conf.get("target.keywords", "shooting,swimming,...");
        // if (keywordsStr != null && !keywordsStr.isEmpty()) {
        //     targetKeywords = Arrays.asList(keywordsStr.toLowerCase().split(","));
        // }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line == null || line.trim().isEmpty()) {
            return;
        }

        if (textColumnIndex == -1 && !headerProcessedByThisMapper) {
            headerProcessedByThisMapper = true;
            String[] headerParts = line.split(csvDelimiter); // Use proper CSV parsing for robustness
            List<String> headerList = Arrays.asList(headerParts);
            boolean foundColumn = false;
            for (int i = 0; i < headerList.size(); i++) {
                if (headerList.get(i).trim().equalsIgnoreCase(targetColumnName)) {
                    textColumnIndex = i;
                    foundColumn = true;
                    break;
                }
            }
            if (foundColumn) {
                return; // Skip header line from data processing
            } else {
                context.getCounter(MAPPER_COUNTERS.COLUMN_NOT_FOUND_IN_HEADER).increment(1);
                // textColumnIndex remains -1, subsequent lines for this mapper will be skipped.
            }
        }

        if (textColumnIndex == -1) {
            context.getCounter(MAPPER_COUNTERS.LINES_SKIPPED_NO_HEADER).increment(1);
            return;
        }

        String[] parts = line.split(csvDelimiter); // Use proper CSV parsing for robustness

        if (parts.length > textColumnIndex) {
            String tweetTextOriginal = parts[textColumnIndex].trim();
            if (tweetTextOriginal.isEmpty()) {
                return;
            }
            String tweetTextLowercase = tweetTextOriginal.toLowerCase(); // Convert tweet to lowercase once

            for (String keyword : targetKeywords) {
                // The keywords in targetKeywords list are already lowercase
                if (tweetTextLowercase.contains(keyword)) {
                    keywordOutputKey.set(keyword); // Set the keyword as the key
                    context.write(keywordOutputKey, one);
                }
            }
        } else {
            context.getCounter(MAPPER_COUNTERS.INVALID_ROW_SHORT).increment(1);
        }
    }
}
