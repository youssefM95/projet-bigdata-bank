package projet_bigdata_bank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxIncome {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, FloatWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			if (value.toString().contains("id")) return;
			
			String line = value.toString();
			String columns[] = line.split(",");
			
			context.write(new Text("max"), new FloatWritable(Float.parseFloat(columns[4])));
		}
	}

	public static class FloatMaxReducer extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float max = Float.parseFloat( values.iterator().next().toString());
			
			for (FloatWritable val : values) {
				if (max < val.get()) max = val.get();
			}
			result.set(max);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "max income");
		job.setJarByClass(MaxIncome.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(FloatMaxReducer.class);
		job.setReducerClass(FloatMaxReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
