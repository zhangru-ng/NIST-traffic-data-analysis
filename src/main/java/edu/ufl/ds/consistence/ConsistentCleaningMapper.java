package edu.ufl.ds.consistence;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConsistentCleaningMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	static final int JAM_THRESHOLD = 80;
	static final float FACTOR_2D = 4.47f;
	static final float FACTOR_3D_THRESHOLD = 500;
	private Text outputKey = new Text();
	private Text outputVal = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		/*
		 * schema [0] lane_id, [1] measurement_start, [2] speed, [3] flow, [4]
		 * occupancy, [5] quality 0 - valid, 1 - invalid, 2 - incomplete, 3 -
		 * unknown
		 */
		String[] parts = value.toString().split(",", 6);
		// if it is considered as wrong in previous job, no need to check it
		// again
		outputKey.set(parts[0]);

		String[] tab_parts = value.toString().split("\t");
		if (tab_parts[1].equals("0")) {
			outputVal.set(value.toString());
			context.write(outputKey, outputVal);
			return;
		}

		int speed = Integer.parseInt(parts[2]);
		int flow = Integer.parseInt(parts[3]);
		int occupancy = Integer.parseInt(parts[4]);
		// group by lane_id
		// mean and std cleaning can be done in each group
		String reason = "";
		// mark record as invalid before checking
		String correctness = "0";
		if (speed < 0 || flow < 0) {
			reason = "Negative speed or flow";
		}
		// flow = 0 means number of cars pass through in this interval is 0
		// Since no car passes, we can't get average speed
		else if (flow == 0 && speed > 0) {
			reason = "Flow is zero but average speed is greater than 0";
		}
		// when speed is 0 and flow is greater than 0, there should be a jam
		else if (speed == 0 && flow > 0 && occupancy < JAM_THRESHOLD) {
			reason = "Inconsistent speed and flow";
		}
		// when both flow and speed are 0, there may be no car at all or a jam
		// note that speed is an integer and in mph, speed = 0 may means speed
		// is less than 1 mph, the vehicle does not necessarily stop.
		else if (flow == 0 && speed == 0 && occupancy > 0
				&& occupancy < JAM_THRESHOLD) {
			reason = "Inconsistent occupancy when both flow and speed are 0";
		}
		// 2D relationship between speed and flow
		// Given the minimum effective vehicle length (the sum of actual vehicle
		// length
		// and detectable length of loop detector), we can calculate the
		// theoretical maximum
		// number of cars that can pass through a detector with a given speed
		// max(flow) = Interval(s) * 0.447 * speed(mph) / min(effective vehicle
		// length(m))
		else if (speed * FACTOR_2D < flow) {
			reason = "Inconsistent speed and flow";
		}
		// 3D relationship between speed, flow, and occupancy
		// According to traffic flow theory
		// v = c * q / o
		// v -- space mean speed(in mph)
		// q -- flow
		// o -- occupancy
		// c -- mean effective vehicle length(in feet) * 1.894
		// For example:
		// compact sedans length is about 14.75 feet
		// truck length is about is about 50 feet
		// The normal detectable length of loop detector is about 5 feet
		// so for sedans c is about 37, and for truck, c is about 104.17
		else {
			float coefficient = flow > 0 ? speed * occupancy / flow : 0.0f;
			if ((coefficient > 0 && coefficient < 1)
					|| (coefficient > FACTOR_3D_THRESHOLD && occupancy < JAM_THRESHOLD)
					|| (coefficient > 4 * FACTOR_3D_THRESHOLD && occupancy >= JAM_THRESHOLD)) {
				reason = "Inconsistent speed, flow and occupancy";
			}
			// records reach this branch are considered as consistent
			else {
				correctness = "1";
			}

		}
		String suffix = correctness.equals("1") ? correctness : correctness
				+ "\t\"" + reason + "\"";
		outputVal.set(tab_parts[0] + '\t' + suffix);
		context.write(outputKey, outputVal);
	}
}
