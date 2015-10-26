package edu.ufl.ds.join;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JoinDriver {
    private static String outBucket = "";
    private static String tmp = "";
    private static String result = "";
    private static String resultPrefix = "joined_";

    public static void join(Path inputPath1, Path inputPath2) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
	Configuration conf = new Configuration();

	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
	job.setJarByClass(JoinDriver.class);

	MultipleInputs.addInputPath(job, inputPath1,  TextInputFormat.class, InventoryMapper.class);
	MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, CleanTestMapper.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setReducerClass(JoinReducer.class);

	String cleanTestName = inputPath2.getName();
	Path outputPath = new Path(tmp + cleanTestName);
	Path resultPath = new Path(result + resultPrefix + cleanTestName);

	job.setOutputFormatClass(TextOutputFormat.class);
	FileOutputFormat.setOutputPath(job, outputPath);

	job.waitForCompletion(true);

	FileSystem fs = FileSystem.get(new URI(outBucket), conf);
	FileUtil.copyMerge(fs, outputPath, fs, resultPath, false, conf, "");
    }
    /*
     * args[0]: Inventory file path
     * args[1]: Cleaning test directory path
     * args[2]: Output bucket(directory) path
     */
    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] paths = new GenericOptionsParser(conf, args).getRemainingArgs();

	Path inputPath1 = new Path(paths[0]);
	Path inputPath2 = new Path(paths[1]);
	outBucket = paths[2];
	tmp = paths[2] + "tmp/";
	result = paths[2] + "result/";

	FileSystem fs = FileSystem.get(new URI(paths[2]), conf);
	Path tempPath = new Path(tmp);
	if (fs.exists(tempPath)) {
	    fs.delete(tempPath, true);
	}
	// run on all cleaning_test file, run each file separately to preserve file name in result
	RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(inputPath2, false);
	while (fileIterator.hasNext()) {
	    LocatedFileStatus stat = fileIterator.next();
	    JoinDriver.join(inputPath1, stat.getPath());
	}
    }
}
