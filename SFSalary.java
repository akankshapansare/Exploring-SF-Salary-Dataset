package com.sf;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SFSalary extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new SFSalary(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "SFSalary");
		job.setJarByClass(SFSalary.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(SFSalaryMapper.class);
		job.setReducerClass(SFSalaryReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class SFSalaryMapper extends
			Mapper<LongWritable, Text, Text, FloatWritable> {

		@Override
		public void map(LongWritable id, Text inputLine, Context context)
				throws IOException, InterruptedException {
			String str[] = inputLine.toString().split(";");

			Text jobTitle = new Text();
			jobTitle.set(str[2].toUpperCase());

			FloatWritable totalPay = new FloatWritable();
			if (!str[7].isEmpty() && Float.parseFloat(str[7]) > 0) {
				totalPay.set(Float.parseFloat(str[7]));

				context.write(jobTitle, totalPay);
			}
		}
	}

	public static class SFSalaryReducer extends
			Reducer<Text, FloatWritable, Text, Text> {
		@Override
		public void reduce(Text jobTitle, Iterable<FloatWritable> totalPays,
				Context context) throws IOException, InterruptedException {
			float sum = 0f;
			int count = 0;
			float max = 0f;
			float min = Float.MAX_VALUE;
			for (FloatWritable val : totalPays) {
				if (val.get() > max) {
					max = val.get();
				}
				if(val.get() < min){
					min = val.get();
				}
				sum += val.get();
				count++;
			}
			float average = sum / count;
			int range = (int) (max - min);
			Text outputValue = new Text();
			outputValue.set("\t" + average + "\t" + max + "\t" + min + "\t" + range);
			context.write(jobTitle, outputValue);
		}
	}
}
