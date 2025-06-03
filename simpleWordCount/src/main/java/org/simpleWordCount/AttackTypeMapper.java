package org.simpleWordCount;

// AttackTypeMapper.java
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AttackTypeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text outputAttackType = new Text();

    // Column indices
    private int attackTypeColIdx = -1;
    private int severityLevelColIdx = -1;
    private int actionTakenColIdx = -1;

    // Header names to look for (will be read from config)
    private String attackTypeHeader;
    private String severityLevelHeader;
    private String actionTakenHeader;

    // Filter values (will be read from config)
    private String filterSeverityValue;
    private String filterActionValue;

    private boolean headersProcessed = false;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        attackTypeHeader = conf.get(AttackFrequencyDriver.ATTACK_TYPE_HEADER_CONF, "Attack Type"); // Default if not set
        severityLevelHeader = conf.get(AttackFrequencyDriver.SEVERITY_LEVEL_HEADER_CONF, "Severity Level");
        actionTakenHeader = conf.get(AttackFrequencyDriver.ACTION_TAKEN_HEADER_CONF, "Action Taken");

        filterSeverityValue = conf.get(AttackFrequencyDriver.FILTER_SEVERITY_VALUE_CONF, "High");
        filterActionValue = conf.get(AttackFrequencyDriver.FILTER_ACTION_VALUE_CONF, "ignored");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (!headersProcessed && key.get() == 0) { // Process header row
            String[] csvHeaders = line.split(","); // Simple CSV split
            for (int i = 0; i < csvHeaders.length; i++) {
                String header = csvHeaders[i].trim();
                if (header.equalsIgnoreCase(attackTypeHeader)) {
                    attackTypeColIdx = i;
                } else if (header.equalsIgnoreCase(severityLevelHeader)) {
                    severityLevelColIdx = i;
                } else if (header.equalsIgnoreCase(actionTakenHeader)) {
                    actionTakenColIdx = i;
                }
            }
            headersProcessed = true;

            // Log if any required header is not found
            if (attackTypeColIdx == -1) System.err.println("Header '" + attackTypeHeader + "' not found.");
            if (severityLevelColIdx == -1) System.err.println("Header '" + severityLevelHeader + "' not found.");
            if (actionTakenColIdx == -1) System.err.println("Header '" + actionTakenHeader + "' not found.");

            return; // Don't process the header row as data
        }

        // Proceed only if all required column indices were found and it's not the header row
        if (attackTypeColIdx != -1 && severityLevelColIdx != -1 && actionTakenColIdx != -1 && key.get() != 0) {
            String[] columns = line.split(","); // Simple CSV split

            // Ensure columns array is long enough before accessing indices
            if (columns.length > attackTypeColIdx &&
                    columns.length > severityLevelColIdx &&
                    columns.length > actionTakenColIdx) {

                String currentSeverity = columns[severityLevelColIdx].trim();
                String currentActionTaken = columns[actionTakenColIdx].trim();

                // Apply the filter conditions (case-insensitive)
                if (currentSeverity.equalsIgnoreCase(filterSeverityValue) &&
                        currentActionTaken.equalsIgnoreCase(filterActionValue)) {

                    String currentAttackType = columns[attackTypeColIdx].trim();
                    if (!currentAttackType.isEmpty()) {
                        outputAttackType.set(currentAttackType);
                        context.write(outputAttackType, one);
                    }
                }
            } else {
                System.err.println("Malformed line or insufficient columns after header: " + line);
            }
        } else if (key.get() != 0) { // It's a data row, but headers weren't found properly
            System.err.println("Skipping data line as one or more required headers were not found. Line: " + line);
        }
    }
}