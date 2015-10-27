package edu.ufl.ds.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputVal = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",", 3);
        outputKey.set(parts[0] + "," + convert(parts[1].substring(8, parts[1].length() - 3)));
        outputVal.set(value);
        context.write(outputKey, outputVal);
    }

    public long convert(String time) {
	String[] dayParts = time.split(" ");
	int day = Integer.parseInt(dayParts[0]);
	String[] milliParts = time.split(".");
	int millis = milliParts.length > 1 ? Integer.parseInt(milliParts[1]) : 0;
	String[] timeParts = milliParts[0].split(":");
	int hour = Integer.parseInt(timeParts[0]);
	int minute = Integer.parseInt(timeParts[1]);
	int second = Integer.parseInt(timeParts[2]);
	long timestamp = (((day * 24L + hour) * 60 + minute) * 60 + second) * 1000 + millis;
	return timestamp;
    }
}
