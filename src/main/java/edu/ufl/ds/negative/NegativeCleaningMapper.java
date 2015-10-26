package edu.ufl.ds.negative;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NegativeCleaningMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String output = value.toString();
		String[] parts = output.split(",");
		context.write(
				new Text(parts[6] + ","
						+ parts[1].substring(0, parts[1].length() - 6)),
				new Text(output));
	}
}
