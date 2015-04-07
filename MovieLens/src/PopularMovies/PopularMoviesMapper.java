package PopularMovies;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 *@author Aishwarya Srinivasan
 */

public class PopularMoviesMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line,"::");
		ArrayList<String> lineElements = new ArrayList<>();
		
		while(st.hasMoreElements()){
			lineElements.add(st.nextElement().toString());
		}
		String movieid = lineElements.get(1);
		String stars = lineElements.get(2);
		
		if(!stars.equals("0")){
			context.write(new Text(movieid), new IntWritable(Integer.parseInt(stars)));
		}
	}
}
