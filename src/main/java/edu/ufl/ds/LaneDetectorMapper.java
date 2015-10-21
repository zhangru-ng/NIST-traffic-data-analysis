package edu.ufl.ds;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LaneDetectorMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value1, Context context)

	throws IOException, InterruptedException {
		System.out.println(value1.toString());
		if (!value1.toString().contains("lane_id")) {
			String[] input = value1.toString().split(",");
			String time = input[1].split(" ")[1].split("-")[0];
			try {

				String myDateString = time;
				SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
				Date date = sdf.parse(myDateString);

				Calendar calendar = GregorianCalendar.getInstance();
				calendar.setTime(date);
				int hour = calendar.get(Calendar.HOUR_OF_DAY);
				System.out.println(hour);
				context.write(
						new Text(input[0] + "," + hour + ","
								+ input[1].split(" ")[0]), new Text(input[2]
								+ "," + input[3] + "," + input[4]));
			} catch (IOException ex) {
				Logger.getLogger(LaneDetectorMapper.class.getName()).log(
						Level.SEVERE, null, ex);
				ex.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
