package MovieRating;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import UserRating.UserRatingCompositeKeyWritable;

/*
 *@author Aishwarya Srinivasan
 */

public class MovieRatingGroupingComparator extends WritableComparator{
	protected MovieRatingGroupingComparator(){
		super(MovieRatingCompositeKeyWritable.class, true);
	}
	
	
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		MovieRatingCompositeKeyWritable key1 = (MovieRatingCompositeKeyWritable) a;
		MovieRatingCompositeKeyWritable key2 = (MovieRatingCompositeKeyWritable) b;
		return key1.getMovieid().compareTo(key2.getMovieid());
	}
}
