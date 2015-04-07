package UserRating;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.Mapper;

/*
 *@author Aishwarya Srinivasan
 */

public class UserRatingMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, "::");
		ArrayList<String> lineElements =  new ArrayList<>();
		while(st.hasMoreElements()){
			lineElements.add(st.nextElement().toString());
		}
		String userid = lineElements.get(0);
		String rating = lineElements.get(2);
		int count = 1;
		if(!rating.equals("0")){
			context.write(new Text(userid), new IntWritable(count));
		}
		
	}

	

}
