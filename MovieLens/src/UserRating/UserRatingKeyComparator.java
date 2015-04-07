package UserRating;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 *@author Aishwarya Srinivasan
 */

public class UserRatingKeyComparator extends WritableComparator{

	protected UserRatingKeyComparator(){
		super(UserRatingCompositeKeyWritable.class,true);
	}
	
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		UserRatingCompositeKeyWritable key1 = (UserRatingCompositeKeyWritable) a;
		UserRatingCompositeKeyWritable key2 = (UserRatingCompositeKeyWritable) b;
		
		int compareresut = key1.getUserid().compareTo(key2.getUserid());
		//int compareresut =0;
		if(compareresut == 0){
			//to get rating count in descending order from highest to lowest
			return -1*key1.getRatingcount().compareTo(key2.getRatingcount());
		}
		return compareresut;
	}
}
