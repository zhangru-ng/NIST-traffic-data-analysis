package edu.ufl.ds.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanTestMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputVal = new Text();
    final static private String fileTag = "CleanTest";
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] input = value.toString().split(",", 2);
        if (!input[0].contains("lane_id")) {
            outputKey.set(input[0]);
            outputVal.set(fileTag + "," + input[1]);
            context.write(outputKey, outputVal);
        }
    }
}
