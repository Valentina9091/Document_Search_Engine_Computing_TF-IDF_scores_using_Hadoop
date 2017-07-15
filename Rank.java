package org.myorg;

import java.io.IOException;
import java.nio.ByteBuffer;

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

import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * @author Valentina Palghadmal 
 * 
 * 
 *         This class ranks the search hits in descending order using their
 *         accumulated score
 *
 */
public class Rank extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Rank.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Rank(), args);
		System.exit(res);
	}

	/**
	 * This method is used to execute the map reduce code
	 */
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), " Rank ");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// job.setNumReduceTasks(1);
		// job.setSortComparatorClass(DoubleComparator.class);
		// job.setGroupingComparatorClass(DoubleComparator.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * 
	 * This class takes the input as filename and score and returns output as
	 * negation of score and filename. This is done to so that we can use the
	 * inbuilt auto sorting by map reduce to sort the values in descending order
	 *
	 */
	public static class Map extends
			Mapper<LongWritable, Text, DoubleWritable, Text> {
		/**
		 * 
		 * This map class is used to read the contains from the file line by
		 * line and generate key value pairs.
		 */
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			if (!line.isEmpty()) {
				String list[] = line.split("\t");
				if (list.length == 2)
					context.write(new DoubleWritable(Double.valueOf(list[1])
							* -1), new Text(list[0]));
			}
		}
	}

	/**
	 * 
	 * This class is used to write the sorted values in file
	 *
	 */
	public static class Reduce extends
			Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		@Override
		public void reduce(DoubleWritable count, Iterable<Text> filename,
				Context context) throws IOException, InterruptedException {
			for (Text file : filename) {
				context.write(file, new DoubleWritable(count.get() * -1));
			}
		}
	}

	/*
	 * public static class DoubleComparator extends WritableComparator {
	 * 
	 * public DoubleComparator() { super(DoubleWritable.class); }
	 * 
	 * @Override public int compare(byte[] b1, int s1, int l1, byte[] b2, int
	 * s2, int l2) {
	 * 
	 * Double v1 = ByteBuffer.wrap(b1, s1, l1).getDouble(); Double v2 =
	 * ByteBuffer.wrap(b2, s2, l2).getDouble();
	 * 
	 * return v1.compareTo(v2) * (-1); } }
	 */

}
