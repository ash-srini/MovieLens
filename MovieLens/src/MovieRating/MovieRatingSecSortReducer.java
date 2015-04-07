package MovieRating;

import java.io.IOException;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*
 *@author Aishwarya Srinivasan
 */

public class MovieRatingSecSortReducer extends Reducer<MovieRatingCompositeKeyWritable, NullWritable, MovieRatingCompositeKeyWritable, NullWritable>{
	
	@Override
	public void reduce(MovieRatingCompositeKeyWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
		
		for(NullWritable value : values){
			context.write(key, NullWritable.get());
			//System.out.println("key is :"+key);
		}
	}
}
