package Mahout;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import MovieLens.MapReduceDriverUtil;

/*
 *@author Aishwarya Srinivasan
 */

public class MahoutMapRedUtil  extends Configured implements Tool{
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		Configuration conf = new Configuration();
		Job mahout = new Job(conf, "Recommender Job");
		
		mahout.setJarByClass(MapReduceDriverUtil.class);
		
		//Set Mapper and reducer class
		mahout.setMapperClass(RecommendationMapper.class);
		mahout.setReducerClass(MahoutReccoReducer.class);
		
		//Specify key/value
		mahout.setOutputKeyClass(Text.class);
		mahout.setOutputValueClass(IntWritable.class);
		
		//Input
		FileInputFormat.addInputPath(mahout, new Path("hdfs://localhost:54310/user/hduser/ml-1m/users_small.dat"));
		mahout.setInputFormatClass(TextInputFormat.class);
			
		//Output
		conf.addResource(new Path("/usr/local/Cellar/hadoop/1.2.1/libexec/conf/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/1.2.1/libexec/conf/hdfs-site.xml"));
		String mahoutfilename = "hdfs://localhost:54310/user/hduser/ml-1m/mahout";
		Path mpath = new Path(mahoutfilename);
		FileSystem mfileSystem = FileSystem.get(conf);
		 if (mfileSystem.exists(mpath)) {
			 mfileSystem.delete(mpath);
		 }
		FileOutputFormat.setOutputPath(mahout, new Path("hdfs://localhost:54310/user/hduser/ml-1m/mahout"));
		mahout.setOutputFormatClass(TextOutputFormat.class);
		
		mahout.waitForCompletion(true);
		System.out.println("Mahout Recco Job complete:mahout");
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}


}
