package org.myorg;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author Valentina Palghadmal 
 * 
 *         This class contains map reduce code to implements a simple batch mode
 *         search engine. The job (Search.java) accepts as input a user query
 *         and outputs a list of documents with scores that best matches the
 *         query
 */
public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Search.class);
	private static String delimeter = "#####";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Search(), args);
		System.exit(res);
	}

	/**
	 * This method is used to execute the map reduce code
	 */
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), " Search ");
		// passed user queries in array format to configuration.
		job.getConfiguration().setStrings("userQuery", args);
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * 
	 * This map class list of documents with scores that best matches the query
	 */
	public static class Map extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		Set<String> keysToSearch;

		/**
		 * Initial setup to assign the user query tokens to set 
		 */
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			String[] userQuery = context.getConfiguration().getStrings(
					"userQuery");
			keysToSearch = new HashSet<String>();
			if (userQuery != null) {
				for (int i = 2; i < userQuery.length; i++) {
					keysToSearch.add(userQuery[i]);
				}
			}
		}

		/**
		 * This method check if the file contains any user specified token
		 */
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString();
			String words[] = line.split(delimeter);
			if (words.length == 2) {
				if (keysToSearch.contains(words[0])) {
					String list[] = words[1].split("\t");
					context.write(new Text(list[0]),
							new DoubleWritable(Double.valueOf(list[1])));
				}
			}
		}
	}

	/**
	 * 
	 * This class compute the score for each file 
	 *
	 */
	public static class Reduce extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		/**
		 * This function is used to write the score for each file based on the user provided search string
		 * @param filename
		 * @param counts
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void reduce(Text filename, Iterable<DoubleWritable> counts,
				Context context) throws IOException, InterruptedException {
			double sum = 0;
			for (DoubleWritable count : counts) {
				sum += count.get();
			}
			context.write(filename, new DoubleWritable(sum));
		}
	}

}
