package PopularMovies;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 *@author Aishwarya Srinivasan
 */

public class PopularMoviesReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int sum = 0;
		int count=0;
		for(IntWritable value : values){
			sum = sum + Integer.parseInt(value.toString());
			count++;
		}
		int avg = sum/count;
		context.write(key, new IntWritable(avg));
	}
}
