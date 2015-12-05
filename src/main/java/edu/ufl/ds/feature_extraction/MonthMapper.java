package edu.ufl.ds.feature_extraction;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MonthMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value1, Context context)

	throws IOException, InterruptedException {
		String input = value1.toString();
		try {
			String[] k = input.split(",");
			context.write(new Text(k[0] + "," + k[1] + "," + k[2] + "," + k[3]
					+ "," + k[4].substring(5)), value1);
		} catch (Exception ex) {
			Logger.getLogger(MonthMapper.class.getName()).log(Level.SEVERE,
					null, ex);
			ex.printStackTrace();
		}
	}
}
