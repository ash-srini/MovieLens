package Mahout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.cf.taste.common.TasteException;

/*
 *@author Aishwarya Srinivasan
 */

public class RecommendationMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, "::");
		ArrayList<String> lineElements =  new ArrayList<>();
		while(st.hasMoreElements()){
			lineElements.add(st.nextElement().toString());
		}
		String userid = lineElements.get(0);
		MahoutRecco mrecco = new MahoutRecco();
		String recco;
		try {
			recco = mrecco.recco(userid);
			System.out.println(userid);
			int count=1;
			context.write(new Text(recco), new IntWritable(count));
			System.out.println(recco);
		} catch (TasteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
	}
}
