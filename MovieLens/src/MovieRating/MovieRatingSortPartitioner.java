package MovieRating;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 *@author Aishwarya Srinivasan
 */

public class MovieRatingSortPartitioner extends Partitioner<MovieRatingCompositeKeyWritable, NullWritable>{

	@Override
	public int getPartition(MovieRatingCompositeKeyWritable mrckw,
			NullWritable value, int numReduceTask) {
		// TODO Auto-generated method stub
	//	System.out.println("mrckw.getMovieid() : "+mrckw.getMovieid()+"mrckw.getRatingcount() : "+mrckw.getRatingcount());
		return (mrckw.getMovieid().hashCode() % numReduceTask);
		
		
	}

	
}
