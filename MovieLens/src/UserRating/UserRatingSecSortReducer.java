package UserRating;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*
 *@author Aishwarya Srinivasan
 */

public class UserRatingSecSortReducer extends Reducer<UserRatingCompositeKeyWritable, NullWritable, UserRatingCompositeKeyWritable, NullWritable>{
	
	@Override
	public void reduce(UserRatingCompositeKeyWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
		for(NullWritable value: values){
			context.write(key, NullWritable.get());
		}
	}

}
