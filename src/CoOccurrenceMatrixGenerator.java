import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CoOccurrenceMatrixGenerator {
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


			String line = value.toString().trim();
			String[] user_movieRating = line.split("\t");


			if(user_movieRating.length != 2){
				return;
			}

			String[] movie_ratings = user_movieRating[1].split(",");


			for (int i=0;i<movie_ratings.length;i++){

				String movie1 = movie_ratings[i].split(":")[0];
				for(int j=0;j<movie_ratings.length;j++){
					String movie2 = movie_ratings[j].split(":")[0];
					String outputKey = movie1 + ":" + movie2;
					context.write(new Text(outputKey), new IntWritable(1));
				}
			}


		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {


			int sum=0;
			for(IntWritable value:values){
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);
		
		job.setJarByClass(CoOccurrenceMatrixGenerator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}
