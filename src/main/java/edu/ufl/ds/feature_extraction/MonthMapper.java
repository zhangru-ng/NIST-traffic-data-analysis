package edu.ufl.ds.feature_extraction;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MonthMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value1, Context context)

	throws IOException, InterruptedException {
		String[] input = value1.toString().split(",");
		try {
			System.out.println(input[0]);
			String[] k = input[0].split(",");
			context.write(new Text(k[0] + "," + k[1].substring(0, 7) + ","
					+ k[7] + "," + k[8]), value1);
		} catch (Exception ex) {
			Logger.getLogger(MonthMapper.class.getName()).log(Level.SEVERE,
					null, ex);
			ex.printStackTrace();
		}
	}
}
