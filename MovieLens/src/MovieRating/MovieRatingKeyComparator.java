package MovieRating;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 *@author Aishwarya Srinivasan
 */

public class MovieRatingKeyComparator extends WritableComparator{

	protected MovieRatingKeyComparator(){
		super(MovieRatingCompositeKeyWritable.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b){
		MovieRatingCompositeKeyWritable key1 = (MovieRatingCompositeKeyWritable) a;
		MovieRatingCompositeKeyWritable key2 = (MovieRatingCompositeKeyWritable) b;
		//System.out.println(key1.getMovieid()+" compare to "+ key2.getMovieid());
		int compareresult = key1.getMovieid().compareTo(key2.getMovieid());
		if(compareresult == 0){
			return -key1.getRatingcount().compareTo(key2.getRatingcount());
		}
		return compareresult;
	}
}
