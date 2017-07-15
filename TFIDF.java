package org.myorg;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.myorg.TermFrequency;

/**
 * 
 * @author Valentina Palghadmal 
 * 
 *         This class calculates the TF-IDF score for each word in the file.
 *
 */
public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);
	private static String delimeter = "#####";

	public static void main(String[] args) throws Exception {
		// Execute the TermFrequency class
		int tf = ToolRunner.run(new TermFrequency(), args);
		// Executing TFIDF class
		int res = ToolRunner.run(new TFIDF(), args);
		System.exit(res);
	}

	/**
	 * This method is used to execute the map reduce code
	 */
	public int run(String[] args) throws Exception {

		// Calculate total number of files
		int count = 0;
		FileSystem fs = FileSystem.get(getConf());
		boolean recursive = false;
		RemoteIterator<LocatedFileStatus> ri = fs.listFiles(new Path(args[0]),
				recursive);
		while (ri.hasNext()) {
			count++;
			ri.next();
		}
		LOG.info("TOTAL NUMBER OF FILES: " + count);
		// Adding Total number of files are a parameter to configuration
		Configuration conf = new Configuration();
		conf.set("TotalNoOfFiles", count + "");

		Job job = Job.getInstance(conf, " TFIDF ");
		job.setJarByClass(this.getClass());
		// The output of TermFrquency class is the input path to TFIDF
		FileInputFormat.addInputPaths(job, args[1]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapperClass(Map2.class);
		job.setReducerClass(Reduce2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * 
	 * This map class is used to read the contains from the file line by line
	 * and generate key value pairs where key -> Text(word) value ->
	 * filename=term frequency
	 */
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		/**
		 * This method is used the read the inputs & generate key value pairs.
		 * The output of TermFrquency class is the input to the mapper
		 * 
		 * @param offset
		 *            : LongWritable
		 * @param lineText
		 *            : Text
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString().toLowerCase();
			String words[] = line.split(delimeter);
			if (words.length == 2) {
				String list[] = words[1].split("\t");
				Text value = new Text(list[0] + "=" + list[1]);
				context.write(new Text(words[0]), value);
			}
		}
	}

	/**
	 * 
	 * This reducer class takes the input from the mapper and generates output
	 * as word#####filename TFIDF
	 *
	 */
	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		/**
		 * This method takes the input from the map class and displays the TF-IDF
		 * of each word in a file
		 * 
		 * @param word
		 *            : Text format
		 * @param counts
		 *            : Iterable<Text>
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context)
				throws IOException, InterruptedException {
			// Getting Total number of files from configuration.
			Configuration conf = context.getConfiguration();
			double totalNoOfFiles = Double.valueOf(conf.get("TotalNoOfFiles"));
			Map<String, Double> map = new HashMap<String, Double>();
			int fileCounts = 0;
			for (Text value : counts) {
				fileCounts++;
				map.put(value.toString().split("=")[0],
						Double.valueOf(value.toString().split("=")[1]));

			}
			// Calculating IDF value
			double termIDF = Math.log10(1 + (totalNoOfFiles / fileCounts));
			double tfidf = 0;
			for (String key : map.keySet()) {
				//calculating TFIDF score
				tfidf = termIDF * map.get(key);
				context.write(new Text(word + delimeter + key),	new Text(String.valueOf(tfidf)));

			}
		}
	}

}
