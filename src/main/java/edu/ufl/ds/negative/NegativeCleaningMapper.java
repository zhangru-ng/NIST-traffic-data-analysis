package edu.ufl.ds.negative;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NegativeCleaningMapper extends
		Mapper<LongWritable, Text, Text, Text> {
    	private Text outputKey = new Text();
    	private Text outputVal = new Text();
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String output = value.toString();
		String[] parts = output.split(",");
		String val = StringUtils.join(Arrays.copyOfRange(parts, 0, 7), ",");
		outputKey.set(parts[6] + "," + parts[1].substring(0, 16));
		outputVal.set(val + "," + parts[16] + "," + parts[17]);
		context.write(outputKey, outputVal);
	}
}
