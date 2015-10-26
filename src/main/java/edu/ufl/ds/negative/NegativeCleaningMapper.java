package edu.ufl.ds.negative;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NegativeCleaningMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputVal = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	/*
	 * schema
	 * [0] lane_id,
	 * [1] measurement_start,
	 * [2] speed,
	 * [3] flow,
	 * [4] occupancy,
	 * [5] quality 0 - valid, 1 - invalid, 2 - incomplete, 3 - unknown
	 */
	String[] parts = value.toString().split(",", 7);
	String datetime = parts[1];
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	Date date;
	try {
	    date = format.parse(datetime);
	    Calendar c = Calendar.getInstance();
	    c.setTime(date);
	    // if it is considered as wrong in previous job, no need to check it again
	    outputKey.set(parts[6] + "," + c.get(Calendar.YEAR) + c.get(Calendar.MONTH) + c.get(Calendar.DATE) +
		    c.get(Calendar.HOUR_OF_DAY) + c.get(Calendar.MINUTE));
	    outputVal.set(value);

	    context.write(outputKey, outputVal);
	} catch (ParseException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

    }
}
