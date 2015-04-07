package UserRating;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 *@author Aishwarya Srinivasan
 */

public class UserRatingSecSortMapper extends Mapper<LongWritable, Text, UserRatingCompositeKeyWritable, NullWritable>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		if(value.toString().length() > 0){
			String[] userRatingsAttr = value.toString().split("::");
			context.write(new UserRatingCompositeKeyWritable(userRatingsAttr[0].toString(), userRatingsAttr[2].toString()+userRatingsAttr[1].toString()), NullWritable.get());
		}
		
	}
}
