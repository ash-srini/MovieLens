package UserRating;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;



/*
 *@author Aishwarya Srinivasan
 */

public class UserRatingCompositeKeyWritable<T> implements Writable, WritableComparable<UserRatingCompositeKeyWritable>{
	

	private String userid;
	private String ratingmoviepair;
	
	public UserRatingCompositeKeyWritable(){
		
	}
	public UserRatingCompositeKeyWritable(String userid, String ratingcount){
		this.userid=userid;
		this.ratingmoviepair=ratingcount;
	}
	
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}
	public String getRatingcount() {
		return ratingmoviepair;
	}
	public void setRatingcount(String ratingcount) {
		this.ratingmoviepair = ratingcount;
	}
	
	
	public int compareTo(UserRatingCompositeKeyWritable urcc) {
		int result = userid.compareTo(urcc.getUserid());
		if(result == 0){
			result = ratingmoviepair.compareTo(urcc.getRatingcount());
		}
		return result;
	}
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		userid = WritableUtils.readString(dataInput);
		ratingmoviepair = WritableUtils.readString(dataInput);
		
	}
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, userid);
		WritableUtils.writeString(dataOutput, ratingmoviepair);
		
	}
	
	@Override
	public String toString() {
		return (new StringBuilder().append(userid).append("\t").append(ratingmoviepair).toString());
	};
	


	

}
