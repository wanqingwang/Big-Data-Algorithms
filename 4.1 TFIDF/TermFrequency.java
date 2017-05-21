package tfidf;


import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;



public class TermFrequency extends Configured implements Tool {


	public static class TFMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE 	   = new IntWritable(1);
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		/* Overridden map method */
		@Override
  		public void map(LongWritable offset, Text lineText, Context context)
  							throws IOException, InterruptedException {
			String line;
			String fileName;
			String currentTerm;
			
			// 1. Convert to lower case so that same word written in cases can 
			// be mapped to the same key.
			// 2. Remove tabs with spaces, otherwise it can be conflict with tab separator that separates output key-value pair.
			line = lineText.toString().toLowerCase();
			line = line.replaceAll("\\t", " ");
			
			// Get file name using meta information of input split.
			fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

			// Split every line into tokens.
			// Create a new key and write (new key, 1) to context object.
			for (String term : WORD_BOUNDARY.split(line)) {
				if (term.trim().isEmpty()) {
					continue;
				}
				currentTerm = term.trim() + "_" + fileName;
				context.write(new Text(currentTerm), ONE);
			}
		}
	}


	public static class TFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

		/* Overridden reduce method */
		@Override 
		public void reduce(Text term, Iterable<IntWritable> counts, Context context)
								throws IOException, InterruptedException {
			double termFrequency = 0;

			// Aggregate word counts for a same key.
			for (IntWritable count : counts) {
				termFrequency += (double) count.get();
			}

			// Compute Term Frequency
			if (termFrequency > 0) {
				termFrequency = 1 + Math.log10(termFrequency);
			} else {
				termFrequency = 0;
			}

			context.write(term, new DoubleWritable(termFrequency));
		}
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		
		job.setJarByClass(this.getClass());
		job.setJobName(" TermFrequency ");
		job.setMapperClass(TFMap.class);
		job.setReducerClass(TFReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileSystem fs = FileSystem.get(getConf());
		Path TFPath   = new Path(args[1]);
		fs.delete(TFPath);
		
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, TFPath);
		
		return job.waitForCompletion(true)? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String input;
		String output;
		int status;
		
		// Read input and output paths from users.
		input  = args[0];
		output = args[1];
		
		// Call driver function.
		status = ToolRunner.run(new TermFrequency(), new String[] {input, output});
		
		System.exit(status);
	}

}