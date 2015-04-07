package Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 *@author Aishwarya Srinivasan
 */

public class ReadFromHDFS {

	public static void main(String[] args){
		
		try{
			Configuration conf = new Configuration();
			conf.addResource(new Path("/usr/local/Cellar/hadoop/1.2.1/libexec/conf/core-site.xml"));
			conf.addResource(new Path("/usr/local/Cellar/hadoop/1.2.1/libexec/conf/hdfs-site.xml"));
			String filename = "hdfs://localhost:54310/user/hduser/ml-1m/op/part-r-00000";
			Path p = new Path(filename);
			FileSystem fileSystem = FileSystem.get(conf);
			 if (fileSystem.exists(p)) {
				FSDataInputStream ips = fileSystem.open(p);
				Scanner sc = new Scanner(ips);
				int count = 0;
				System.out.println("exists");
				while(sc.hasNext()){
					
					count++;
					sc.nextLine();
				}
				System.out.println(count);	
			 }
			 
			
		}catch(Exception e){
			System.out.println(e);
		}
	}
}
