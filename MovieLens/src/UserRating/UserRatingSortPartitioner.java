package UserRating;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 *@author Aishwarya Srinivasan
 */

public class UserRatingSortPartitioner extends Partitioner<UserRatingCompositeKeyWritable, NullWritable>{

	@Override
	public int getPartition(UserRatingCompositeKeyWritable urcc,
			NullWritable value, int numReduceTask) {
		
		return (urcc.getUserid().hashCode() % numReduceTask) ;
	}

}
