package MovieRating;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*
 *@author Aishwarya Srinivasan
 */

public class MovieRatingReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		
		int count = 0;
		for(IntWritable value : values){
			
			count++;
		}
		
		context.write(key, new IntWritable(count));
	}
}
