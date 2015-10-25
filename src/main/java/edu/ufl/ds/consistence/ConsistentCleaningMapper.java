package edu.ufl.ds.consistence;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConsistentCleaningMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputVal = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
	// schema
	// lane_id,measurement_start,speed,flow,occupancy,quality
        String[] input = value.toString().split(",", 2);

    }
}
