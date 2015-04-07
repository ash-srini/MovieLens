package UserRating;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 *@author Aishwarya Srinivasan
 */

public class UserRatingGroupingComparator extends WritableComparator{
	
	protected UserRatingGroupingComparator(){
		super(UserRatingCompositeKeyWritable.class, true);
	}
	
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		UserRatingCompositeKeyWritable key1 = (UserRatingCompositeKeyWritable) a;
		UserRatingCompositeKeyWritable key2 = (UserRatingCompositeKeyWritable) b;
		return key1.getUserid().compareTo(key2.getUserid());
	}
}
