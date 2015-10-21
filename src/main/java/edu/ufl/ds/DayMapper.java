package edu.ufl.ds;

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

public class DayMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value1, Context context)

	throws IOException, InterruptedException {
		System.out.println("---" + value1.toString());
		String[] input = value1.toString().split(":");
		try {
			System.out.println(input[0]);
			String[] k = input[0].split(",");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date date = sdf.parse(k[2]);

			Calendar calendar = GregorianCalendar.getInstance();
			calendar.setTime(date);
			int dayOfTheWeek = calendar.get(Calendar.DAY_OF_WEEK);
			System.out.println(dayOfTheWeek);
			context.write(new Text(k[0] + "," + k[1] + "," + dayOfTheWeek),
					new Text(input[1]));
		} catch (Exception ex) {
			Logger.getLogger(DayMapper.class.getName()).log(Level.SEVERE, null,
					ex);
			ex.printStackTrace();
		}
	}
}
