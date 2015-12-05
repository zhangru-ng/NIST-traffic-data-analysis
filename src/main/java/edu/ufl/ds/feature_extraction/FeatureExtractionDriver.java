package edu.ufl.ds.feature_extraction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FeatureExtractionDriver {

	public static void extractionDriver(String inputDir, String outputDir, String boundingBoxesFile)
			throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(outputDir + "output"), conf);

		FileSystem fss = FileSystem.get(new URI(boundingBoxesFile), conf);
		StringBuffer nearbyBuffer = new StringBuffer();
		Path pt = new Path(boundingBoxesFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fss.open(pt)));
		String line;
		line = br.readLine();
		while (line != null) {
			nearbyBuffer.append("#" + line);
			line = br.readLine();
		}
		String boundingBoxes = nearbyBuffer.toString().substring(1);
		conf.set("boundingBoxes", boundingBoxes);
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf);
		job.setJarByClass(FeatureExtractionDriver.class);

		job.setMapperClass(FeatureExtractionMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(FeatureExtractionReducer.class);
		job.setNumReduceTasks(1);

		Path tmpPath = new Path(outputDir + "temp");

		if (fs.exists(tmpPath)) {
			fs.delete(tmpPath, true);
		}

		FileInputFormat.addInputPath(job, new Path(inputDir));
		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, tmpPath);

		job.waitForCompletion(true);

	}

	public static void main(String[] args)
			throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {

		FeatureExtractionDriver.extractionDriver(args[0], args[1], args[2]);
	}
}
