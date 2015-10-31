package edu.ufl.ds.feature_extraction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Main(), args);
	}

	@Override
	public int run(String args[]) {
		try {
			long time = System.currentTimeMillis();
			// "/Users/vipulmittal/Documents/fall-2015/ds/NIST/NIST_traffic_data_analysis/resources/cleaned_data",
			// "/Users/vipulmittal/Documents/fall-2015/ds/NIST/NIST_traffic_data_analysis/resources/temp/output",

			Job job = createJob(
					"/Users/vipulmittal/Documents/fall-2015/ds/NIST/NIST_traffic_data_analysis/resources/features",
					"/Users/vipulmittal/Documents/fall-2015/ds/NIST/NIST_traffic_data_analysis/resources/result",
					MonthMapper.class, MonthReducer.class);

			job.waitForCompletion(true);
		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
		return 0;
	}

	public Job createJob(String inputPath, String outputPath, Class mapper,
			Class reducer) throws IOException {
		Configuration conf = new Configuration();

		conf.set(
				"io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf);
		job.setJarByClass(Main.class);

		job.setMapperClass(mapper);

		job.setReducerClass(reducer);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// specify input and output DIRECTORIES
		FileInputFormat.addInputPath(job, new Path(inputPath));

		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setOutputFormatClass(TextOutputFormat.class);
		return job;
	}
}
