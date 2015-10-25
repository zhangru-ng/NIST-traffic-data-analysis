package edu.ufl.ds.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InventoryMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputVal = new Text();
    final static private String fileTag = "Inventory";
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
	// schema
	// "lane_id","zone_id","lane_number","name","state","road","direction",
	// "location_description","lane_type","organization","detector_type","latitude",
	// "longitude","bearing","default_speed","interval"
        String[] input = value.toString().split(",", 2);
        if (!input[0].contains("lane_id")) {
            outputKey.set(input[0]);
            outputVal.set(fileTag + "," + input[1]);
            context.write(outputKey, outputVal);
        }
    }
}
