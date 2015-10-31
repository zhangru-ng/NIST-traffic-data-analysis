package edu.ufl.ds.feature_extraction;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FeatureExtractionMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	private Text outputKey = new Text();
	private Text outputVal = new Text();

	static class Box {
		double lat1, long1, lat2, long2;

		public double getLat1() {
			return lat1;
		}

		public void setLat1(double lat1) {
			this.lat1 = lat1;
		}

		public double getLong1() {
			return long1;
		}

		public void setLong1(double long1) {
			this.long1 = long1;
		}

		public double getLat2() {
			return lat2;
		}

		public void setLat2(double lat2) {
			this.lat2 = lat2;
		}

		public double getLong2() {
			return long2;
		}

		public void setLong2(double long2) {
			this.long2 = long2;
		}

		public Box(double lat1, double long1, double lat2, double long2) {
			super();
			this.lat1 = lat1;
			this.long1 = long1;
			this.lat2 = lat2;
			this.long2 = long2;
		}

		public Box(String z) {
			String[] temp = z.split("\t");

			this.lat1 = Double.parseDouble(temp[0]);
			this.long1 = Double.parseDouble(temp[1]);
			this.lat2 = Double.parseDouble(temp[2]);
			this.long2 = Double.parseDouble(temp[3]);

		}

		public boolean isInBox(double lat, double log) {
			return lat > lat1 && lat < lat2 && log > long1 && log < long2;
		}

		@Override
		public String toString() {
			return lat1 + "," + long1 + "," + lat2 + "," + long2;
		}
	}

	private static ArrayList<Box> boundingBoxes = null;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println(value.toString());
		try {
			if (boundingBoxes == null) {
				boundingBoxes = new ArrayList<>();
				Configuration conf = context.getConfiguration();
				String[] nearbyStr = conf.get("boundingBoxes").split("#");
				for (String z : nearbyStr) {
					boundingBoxes.add(new Box(z));
				}
			}
			String input = value.toString();
			String regex = "^\\d+$";

			if (!input.contains("created_tstamp")) {

				// String[] inputParts = input.split("\t");
				// if (parts[0].trim().matches(regex)) {
				// for (Box box : boundingBoxes) {
				// if (box.isInBox(Double.parseDouble(parts[7]),
				// Double.parseDouble(parts[8]))) {
				// String tempFlow = parts[3];
				// if (inputParts[1].equals("0")) {
				// tempFlow = inputParts[2];
				// }
				// outputKey.set(box + ":" + parts[1].substring(0, 7));
				// outputVal.set("flow," + tempFlow);
				// context.write(outputKey, outputVal);
				// outputKey.set(box + ":" + parts[1].substring(0, 7));
				// outputVal.set("speed," + parts[2]);
				// context.write(outputKey, outputVal);
				// outputKey.set(box + ":" + parts[1].substring(0, 7));
				// outputVal.set("occupancy," + parts[4]);
				// context.write(outputKey, outputVal);
				//
				// }
				// }
				// } else {
				String[] parts = input.split(",");
				for (Box box : boundingBoxes) {
					if (box.isInBox(Double.parseDouble(parts[3]),
							Double.parseDouble(parts[4]))) {
						outputKey.set(box + ":" + parts[1].substring(0, 7));
						outputVal.set(parts[2] + ",1");
						context.write(outputKey, outputVal);
					}
				}
				// }
			}
		} catch (Exception e) {
			System.out.println(value.toString());
			e.printStackTrace();
		}
	}
}
