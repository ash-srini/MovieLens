package MovieRating;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 *@author Aishwarya Srinivasan
 */

public class MovieRatingSecSortMapper extends Mapper<LongWritable, Text, MovieRatingCompositeKeyWritable, NullWritable>{

	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
		if(value.toString().length() > 0){
			String[] movieRatingAttr = value.toString().split("::");
			context.write(new MovieRatingCompositeKeyWritable(movieRatingAttr[1].toString(), movieRatingAttr[2].toString()+movieRatingAttr[0].toString()), NullWritable.get());
			//System.out.println(movieRatingAttr[0].toString()+"\\t"+movieRatingAttr[1].toString());
		}
	}
}
