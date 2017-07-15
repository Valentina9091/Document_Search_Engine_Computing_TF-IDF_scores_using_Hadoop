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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author Valentina Palghadmal 
 * 
 *         This class contains map reduce code to count the counts the words in
 *         the each file and returns output as word#####filename
 *         logarithmic_TermFrequency of the word.
 */
public class TermFrequency extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TermFrequency.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TermFrequency(), args);
		System.exit(res);
	}

	/**
	 * This method is used to execute the map reduce code
	 */
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " termFrequency ");
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
	 * This map class is used to read the contains from the file line by line
	 * and generate key value pairs where 
	 * key -> Text(word#####filename) 
	 * value  -> DoubleWritable(1)
	 */
	public static class Map extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		private final static DoubleWritable one = new DoubleWritable(1);
	
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

			String line = lineText.toString();
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
				currentWord = new Text((word + "#####" + filename).toLowerCase());
				context.write(currentWord, one);
			}
		}
	}

	/**
	 * 
	 * This reducer class takes the input from the mapper and generates output
	 * as word#####filename logarithmic_TermFrquency
	 *
	 */
	public static class Reduce extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		/**
		 * This method takes the input from the map class and displays the logarithmic Term Frequency
		 * of each word in a file
		 * 
		 * @param word
		 *            : Text format
		 * @param counts
		 *            : Iterable<DoubleWritable>
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void reduce(Text word, Iterable<DoubleWritable> counts,
				Context context) throws IOException, InterruptedException {
			double sum = 0;
			for (DoubleWritable count : counts) {
				sum += count.get();
			}

			// Calculating logarithmic term frequency
			double tfValue = 0;
			if (sum > 0)
				tfValue = 1 + Math.log10(sum);
			context.write(word, new DoubleWritable(tfValue));

		}
	}
}
