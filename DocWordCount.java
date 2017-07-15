package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * 		   @author Valentina Palghadmal
 * 
 *         
 *
 *         This class contains map reduce code to count the words in
 *         the each file and returns output as word#####filename count. This
 *         program is an extension of the WordCount with difference in output.
 */
public class DocWordCount extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(DocWordCount.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new DocWordCount(), args);
		System.exit(res);
	}

	/**
	 * This method is used to execute the map reduce code
	 */
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " wordcount ");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * 
	 * This map class is used to read the contains from the file line by line
	 * and generate key value pairs where key -> Text(word#####filename) value
	 * -> IntWritable(1)
	 */
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		// private Text word = new Text();
		private String delimiter = "#####";
		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\s*\\b\\s*");

		/**
		 * This method is used the read the inputs and generate key value pairs
		 * key -> Text(word#####filename) value -> IntWritable(1)​
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
			Text currentWord = new Text();

			for (String word : WORD_BOUNDARY.split(line)) {
				// Ignore if word is empty string
				if (word.isEmpty()) {
					continue;
				}
				// getting the current file name
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				String filename = fileSplit.getPath().getName();
				// adding delimiter between word and filename
				currentWord = new Text(word + delimiter + filename);
				context.write(currentWord, one);
			}
		}
	}

	/**
	 * 
	 * This reducer class takes the input from the mapper and generates output
	 * as word#####filename count
	 *
	 */
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		/**
		 * This method takes the input from the map class and displays the count
		 * of each word in a file
		 * 
		 * @param word
		 *            : Text format
		 * @param counts
		 *            : Iterable<IntWritable>
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			// Calculates the sum
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}
}
