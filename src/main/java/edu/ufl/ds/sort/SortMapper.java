package edu.ufl.ds.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.ufl.ds.sort.SortDriver.LaneIdAndTimePair;

public class SortMapper extends Mapper<LongWritable, Text, LaneIdAndTimePair, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",", 3);
        int lane_id = Integer.parseInt(parts[0]);
        long timestamp = convert(parts[1].substring(8, parts[1].length() - 3));
        context.write(new LaneIdAndTimePair(lane_id, timestamp), value);
    }

    public long convert(String time) {
	String[] dayParts = time.split(" ");
	int day = Integer.parseInt(dayParts[0]);
	String[] milliParts = dayParts[1].split("\\.");
	int millis = 0;
	if (milliParts.length > 1) {
	    millis = Integer.parseInt(String.format("%-3s", milliParts[1]).replace(' ', '0'));
	}
	String[] timeParts = milliParts[0].split(":");
	int hour = Integer.parseInt(timeParts[0]);
	int minute = Integer.parseInt(timeParts[1]);
	int second = Integer.parseInt(timeParts[2]);
	long timestamp = (((day * 24L + hour) * 60 + minute) * 60 + second) * 1000 + millis;
	return timestamp;
    }
}
