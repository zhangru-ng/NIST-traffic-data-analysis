package edu.ufl.ds.nearby;

import java.io.BufferedReader;
import java.io.FileReader;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NearbyDriver {
    private static String outBucket = "";
    private static String tmp = "";
    private static String result = "";
    private static String resultPrefix = "";
    static StringBuffer nearbyZones = new StringBuffer();


    public static void consistentCleaningDriver(Path inputPath)
	    throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {

	Configuration conf = new Configuration();
	conf.set("nearbyzones", nearbyZones.toString().substring(1));
	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
	job.setJarByClass(NearbyDriver.class);

	job.setMapperClass(NearbyMapper.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setReducerClass(NearbyReducer.class);

	String cleanTestName = inputPath.getName();
	Path tmpPath = new Path(tmp + cleanTestName);

	FileSystem fs = FileSystem.get(new URI(outBucket), conf);
	if (fs.exists(tmpPath)) {
	    fs.delete(tmpPath, true);
	}
	Path resultPath = new Path(result + resultPrefix + cleanTestName);

	FileInputFormat.addInputPath(job, inputPath);
	job.setInputFormatClass(TextInputFormat.class);

	job.setOutputFormatClass(TextOutputFormat.class);
	FileOutputFormat.setOutputPath(job, tmpPath);

	job.waitForCompletion(true);
	FileUtil.copyMerge(fs, tmpPath, fs, resultPath, true, conf, "");
    }
    /*
     * args[0]: Cleaning test directory path
     * args[1]: Output bucket(directory) path
     */
    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] paths = new GenericOptionsParser(conf, args).getRemainingArgs();

	Path inputPath = new Path(paths[0]);
	outBucket = paths[1];
	tmp = outBucket + "tmp/";
	result = outBucket + "result/";

	FileSystem fs = FileSystem.get(new URI(paths[1]), conf);
	try{
	    BufferedReader br = new BufferedReader(new FileReader("resources/nearby.csv"));

	    String line;
	    line = br.readLine();
	    while (line != null){
		nearbyZones.append("#" + line);
		line = br.readLine();
	    }
	}catch(Exception e){
	    e.printStackTrace();
	}
	// run on all cleaning_test file, run each file separately to preserve file name in result
	RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(inputPath, false);
	while (fileIterator.hasNext()) {
	    LocatedFileStatus stat = fileIterator.next();
	    NearbyDriver.consistentCleaningDriver(stat.getPath());
	}
    }
}
