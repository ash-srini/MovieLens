package MovieRating;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/*
 *@author Aishwarya Srinivasan
 */

public class MovieRatingCompositeKeyWritable<T> implements Writable, WritableComparable<MovieRatingCompositeKeyWritable> {

	private String movieid;
	private String ratingmoviepair;
	
	public MovieRatingCompositeKeyWritable(){
		
	}
	
	public MovieRatingCompositeKeyWritable(String movieid, String ratingcount){
		this.movieid = movieid;
		this.ratingmoviepair = ratingcount;
		//System.out.println("ck for "+movieid+" and "+ratingcount+"is created");
	}
	
	public String getMovieid() {
		return movieid;
	}
	public void setMovieid(String movieid) {
		this.movieid = movieid;
	}
	public String getRatingcount() {
		return ratingmoviepair;
	}
	public void setRatingcount(String ratingcount) {
		this.ratingmoviepair = ratingcount;
	}
	@Override
	public int compareTo(MovieRatingCompositeKeyWritable mrckw) {
		int result = movieid.compareTo(mrckw.getMovieid());
		if(result == 0){
			result = ratingmoviepair.compareTo(mrckw.getRatingcount());
		}
		return result;
	}
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		movieid = WritableUtils.readString(dataInput);
		ratingmoviepair = WritableUtils.readString(dataInput);
	}
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, movieid);
		WritableUtils.writeString(dataOutput, ratingmoviepair);
		
	}
	
	@Override
	public String toString() {
		return (new StringBuilder().append(movieid).append("\t").append(ratingmoviepair).toString());
	}
	
}
