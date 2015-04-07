package MovieLens;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Mahout.MahoutReccoReducer;
import Mahout.RecommendationMapper;
import MovieRating.MovieRatingCompositeKeyWritable;
import MovieRating.MovieRatingGroupingComparator;
import MovieRating.MovieRatingKeyComparator;
import MovieRating.MovieRatingMapper;
import MovieRating.MovieRatingReducer;
import MovieRating.MovieRatingSecSortMapper;
import MovieRating.MovieRatingSecSortReducer;
import MovieRating.MovieRatingSortPartitioner;
import PopularMovies.PopularMoviesMapper;
import PopularMovies.PopularMoviesReducer;
import UserRating.UserRatingCompositeKeyWritable;
import UserRating.UserRatingGroupingComparator;
import UserRating.UserRatingKeyComparator;
import UserRating.UserRatingMapper;
import UserRating.UserRatingReducer;
import UserRating.UserRatingSecSortMapper;
import UserRating.UserRatingSecSortReducer;
import UserRating.UserRatingSortPartitioner;

/*
 *@author Aishwarya Srinivasan
 */

public class MapReduceDriverUtil extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
			getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
			}
		//Set job
		Configuration conf = this.getConf();
		Job job = new Job(conf, "UserRating Job");
		job.setJarByClass(MapReduceDriverUtil.class);
		
		//Set Mapper and reducer class
		job.setMapperClass(UserRatingMapper.class);
		job.setReducerClass(UserRatingReducer.class);
		
		//Specify key/value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Input
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310//user/hduser/ml-1m/ratings.dat"));
		job.setInputFormatClass(TextInputFormat.class);
		//Output
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/hduser/ml-1m/output"));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Execute job and return status
		 return job.waitForCompletion(true) ? 0 : 1;

	}
	
	
	public static void main(String[] args)throws Exception{

		try{
		Configuration conf = new Configuration();
		
		/*
		 * This map reduce job gives you the number of ratings given by each user
		 */
		Job job = new Job(conf, "User Rating Job");
		
		job.setJarByClass(MapReduceDriverUtil.class);
		
		//Set Mapper and reducer class
		job.setMapperClass(UserRatingMapper.class);
		job.setReducerClass(UserRatingReducer.class);
		
		//Specify key/value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Input
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/hduser/ml-1m/ratings.dat"));
		job.setInputFormatClass(TextInputFormat.class);
			
		//Output
		conf.addResource(new Path("/usr/local/Cellar/hadoop/1.2.1/libexec/conf/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/1.2.1/libexec/conf/hdfs-site.xml"));
		String filename = "hdfs://localhost:54310/user/hduser/ml-1m/usrrat";
		Path p = new Path(filename);
		FileSystem fileSystem = FileSystem.get(conf);
		 if (fileSystem.exists(p)) {
			 fileSystem.delete(p);
		 }
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/hduser/ml-1m/usrrat"));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
		System.out.println("usrrat Job complete:usrrat");
		
		
		/*
		 * This map reduce job gives the number of rating for each movie
		 */
		Job userRating = new Job(conf, "Movie Rating");
		userRating.setJarByClass(MapReduceDriverUtil.class);
		//Set Mapper and reducer class
		userRating.setMapperClass(MovieRatingMapper.class);
		userRating.setReducerClass(MovieRatingReducer.class);
		
		//Specify key/value
		userRating.setOutputKeyClass(Text.class);
		userRating.setOutputValueClass(IntWritable.class);
		
		//Input
		FileInputFormat.addInputPath(userRating, new Path("hdfs://localhost:54310/user/hduser/ml-1m/ratings.dat"));
		userRating.setInputFormatClass(TextInputFormat.class);
		//Output
		String filename1 = "hdfs://localhost:54310/user/hduser/ml-1m/movrat";
		Path p1 = new Path(filename1);
		
		 if (fileSystem.exists(p1)) {
			 fileSystem.delete(p1);
		 }
		FileOutputFormat.setOutputPath(userRating, new Path("hdfs://localhost:54310/user/hduser/ml-1m/movrat"));
		userRating.setOutputFormatClass(TextOutputFormat.class);
		
		userRating.waitForCompletion(true);
		System.out.println("movrat Job complete: movrat");
		
		
		/*
		 * Secondary sort output sorts the movies rated by the user based on the rating
		 */
		Job topUsers = new Job(conf, "Top Users");
		topUsers.setJarByClass(MapReduceDriverUtil.class);
		FileInputFormat.addInputPath(topUsers, new Path("hdfs://localhost:54310/user/hduser/ml-1m/ratings.dat"));
		FileOutputFormat.setOutputPath(topUsers, new Path("hdfs://localhost:54310/user/hduser/ml-1m/topUsers"));
		
		String filename2 = "hdfs://localhost:54310/user/hduser/ml-1m/topUsers";
		Path p2 = new Path(filename2);
		
		 if (fileSystem.exists(p2)) {
			 fileSystem.delete(p2);
		 }
		
		topUsers.setMapperClass(UserRatingSecSortMapper.class);
		topUsers.setMapOutputKeyClass(UserRatingCompositeKeyWritable.class);
		topUsers.setMapOutputValueClass(NullWritable.class);
		topUsers.setPartitionerClass(UserRatingSortPartitioner.class);
		topUsers.setSortComparatorClass(UserRatingKeyComparator.class);
		topUsers.setGroupingComparatorClass(UserRatingGroupingComparator.class);
		topUsers.setReducerClass(UserRatingSecSortReducer.class);
		topUsers.setOutputKeyClass(UserRatingCompositeKeyWritable.class);
		topUsers.setOutputValueClass(NullWritable.class);
		
		topUsers.waitForCompletion(true);
		
		System.out.println("top users secondary sort Job complete:topUsers");
		
		
		/*
		 * MapReduce job to find average rating stars for each movie
		 */
		Job averageStars = new Job(conf,"averageStars");
		averageStars.setJarByClass(MapReduceDriverUtil.class);
		
		//Set Mapper and reducer class
		averageStars.setMapperClass(PopularMoviesMapper.class);
		averageStars.setReducerClass(PopularMoviesReducer.class);
		
		//Specify key/value
		averageStars.setOutputKeyClass(Text.class);
		averageStars.setOutputValueClass(IntWritable.class);
		
		//Input
		FileInputFormat.addInputPath(averageStars, new Path("hdfs://localhost:54310/user/hduser/ml-1m/ratings.dat"));
		averageStars.setInputFormatClass(TextInputFormat.class);
			
		//Output
		conf.addResource(new Path("/usr/local/Cellar/hadoop/1.2.1/libexec/conf/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/1.2.1/libexec/conf/hdfs-site.xml"));
		String usrratfile = "hdfs://localhost:54310/user/hduser/ml-1m/avgStars";
		Path usrratpath = new Path(usrratfile);
		//FileSystem usrratFileSystem = FileSystem.get(conf);
		 if (fileSystem.exists(usrratpath)) {
			 fileSystem.delete(usrratpath);
		 }
		FileOutputFormat.setOutputPath(averageStars, new Path("hdfs://localhost:54310/user/hduser/ml-1m/avgStars"));
		averageStars.setOutputFormatClass(TextOutputFormat.class);
		
		averageStars.waitForCompletion(true);
		System.out.println("Average rating for each movie Job complete : avgStars");
		
		/*
		 * Secondary sort tell us which user has give a movie the best ratings
		 */
				Job topMovies = new Job(conf, "Top Movies");
				topMovies.setJarByClass(MapReduceDriverUtil.class);
				FileInputFormat.addInputPath(topMovies, new Path("hdfs://localhost:54310/user/hduser/ml-1m/ratings.dat"));
				FileOutputFormat.setOutputPath(topMovies, new Path("hdfs://localhost:54310/user/hduser/ml-1m/topMovies"));
				
				String filemovrat = "hdfs://localhost:54310/user/hduser/ml-1m/topMovies";
				Path p4 = new Path(filemovrat);
				
				 if (fileSystem.exists(p4)) {
					 fileSystem.delete(p4);
				 }
				
				topMovies.setMapperClass(MovieRatingSecSortMapper.class);
				topMovies.setMapOutputKeyClass(MovieRatingCompositeKeyWritable.class);
				topMovies.setMapOutputValueClass(NullWritable.class); 
				topMovies.setPartitionerClass(MovieRatingSortPartitioner.class);
				topMovies.setSortComparatorClass(MovieRatingKeyComparator.class);
				topMovies.setGroupingComparatorClass(MovieRatingGroupingComparator.class);
				topMovies.setReducerClass(MovieRatingSecSortReducer.class);
				topMovies.setOutputKeyClass(MovieRatingCompositeKeyWritable.class);
				topMovies.setOutputValueClass(NullWritable.class);
				//stopMovies.setNumReduceTasks(8);
				topMovies.waitForCompletion(true);
				System.out.println("movie rating secondary sort job complete: topMovies");
				
	/*
	 * Create an hbase table called movieInfoTable with column families as movie name, genre, year, number of ratings and average rating
	 */

				/*
				 * Reading movie.dat file using scanner
				 */
				try {
					String movieTable = "movieInfoTable";
					String[] familys = {"movieName", "genre","releaseYear", "averageRating", "numberOfRatings"};
					HBaseManagementUtil.creatTable(movieTable, familys);
					//HBaseManagementUtil.deleteTable(movieTable);
					HBaseManagementUtil hbmu = new HBaseManagementUtil();
					
					String movinfofname = "/Users/aishwaryasrinivasan/Documents/Big_Data/Assignment3/ml-1m/movies.dat";
					File textfile = new File(movinfofname);
					Scanner in = new Scanner(new FileInputStream(textfile));

					while(in.hasNextLine()){
						Movie movie = new Movie();
						String line = in.nextLine();
						StringTokenizer st = new StringTokenizer(line, "::");
						ArrayList<String> lineElements =  new ArrayList<>();
						while(st.hasMoreElements()){
							lineElements.add(st.nextElement().toString());
							
						}
						movie.setMovieId(lineElements.get(0));
						movie.setMovieName(lineElements.get(1));
						movie.setGenre(lineElements.get(2));
						
						String mName = movie.getMovieName();
						String name;
						String year ;
						//System.out.println("movie name : "+ movie.getMovieName());
						
						if(mName.contains("(")){
							String[] split = movie.getMovieName().split(Pattern.quote("("));
							 name =  split[0].toString();
							 year = split[1].toString().substring(0, split[1].toString().length()-1);
						}else{
							name = mName;
							year="";
						}
						
						hbmu.addRecord(movieTable, movie.getMovieId(), "movieName", "", name);
						hbmu.addRecord(movieTable, movie.getMovieId(), "genre", "", movie.getGenre());
						hbmu.addRecord(movieTable, movie.getMovieId(), "releaseYear", "", year);
					}
					
					//add count of rating from the movrat file to the movieInfoTable
					String hdfsFile = "hdfs://localhost:54310/user/hduser/ml-1m/movrat/part-r-00000";
					Path hdfsfpath = new Path(hdfsFile);
					//Integer rk = 0;
					FileSystem hdfsFS = FileSystem.get(conf);
					if(hdfsFS.exists(hdfsfpath)){
						FSDataInputStream ips1 = fileSystem.open(hdfsfpath);
						Scanner sc1 = new Scanner(ips1);
						while(sc1.hasNextLine()){
							String line = sc1.nextLine();
							StringTokenizer st1 = new StringTokenizer(line, "\t");
							ArrayList<String> lineElems1 = new ArrayList<>();
							while(st1.hasMoreElements()){
								lineElems1.add(st1.nextElement().toString());
							}
							hbmu.addRecord(movieTable, lineElems1.get(0), "numberOfRatings", "", lineElems1.get(1));
						}
					}
					
					
					//add average rating from the avg file to the movieInfoTable
					String dfsFile = "hdfs://localhost:54310/user/hduser/ml-1m/avgStars/part-r-00000";
					Path dfsfpath = new Path(dfsFile);
					//Integer rk = 0;
					FileSystem dfsFS = FileSystem.get(conf);
					if(dfsFS.exists(dfsfpath)){
						FSDataInputStream ips1 = fileSystem.open(dfsfpath);
						Scanner sc1 = new Scanner(ips1);
						while(sc1.hasNextLine()){
							String line = sc1.nextLine();
							StringTokenizer st1 = new StringTokenizer(line, "\t");
							ArrayList<String> lineElems1 = new ArrayList<>();
							while(st1.hasMoreElements()){
								lineElems1.add(st1.nextElement().toString());
							}
							hbmu.addRecord(movieTable, lineElems1.get(0), "averageRating", "", lineElems1.get(1));
						}
					}
					
					hbmu.getAllRecord(movieTable);
					System.out.println("Movie table entered");
				} catch (Exception e) {
					
					e.printStackTrace();
				}
				
			
				/*
				 * Mahout MapReduce Job
				 */
				
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
				
				
//				/*
//				 * Insert the sorted Movie Rating table into Hbase along with genre and year of release
//				 */
//				try{
//				String movieRatingJoinTable = "movieRatingJoinTable";
//				String[] movfamilys = {"movieId", "ratingCount", "genre", "movieName", "releaseYear", "averageRating"};
//				HBaseManagementUtil.creatTable(movieRatingJoinTable, movfamilys);
//				HBaseManagementUtil hbmu = new HBaseManagementUtil();
//				int count = 0;
//				String hdfsFile = "hdfs://localhost:54310/user/hduser/ml-1m/topMovies/part-r-00000";
//				Path hdfsfpath = new Path(hdfsFile);
//				Integer rk = 0;
//				FileSystem hdfsFS = FileSystem.get(conf);
//				if(hdfsFS.exists(hdfsfpath)){
//					FSDataInputStream ips1 = fileSystem.open(hdfsfpath);
//					Scanner sc1 = new Scanner(ips1);
//					while(sc1.hasNextLine()){
//						String line = sc1.nextLine();
//						StringTokenizer st1 = new StringTokenizer(line, "\t");
//						ArrayList<String> lineElems1 = new ArrayList<>();
//						while(st1.hasMoreElements()){
//							lineElems1.add(st1.nextElement().toString());
//						}
//						hbmu.addRecord(movieRatingJoinTable, rk.toString(), "movieId", "", lineElems1.get(0).toString());
//						hbmu.addRecord(movieRatingJoinTable, rk.toString(), "ratingCount", "", lineElems1.get(1).toString());
//						
//						String name;
//						String year;
//						String mName = hbmu.getColumFamilyValue("movieTable", lineElems1.get(0).toString(), "movieName");
//						
//						if(mName.contains("(")){
//							String[]elems =  mName.split(Pattern.quote("("));
//							 name =  elems[0].toString();
//							 year = elems[1].toString().substring(0, elems[1].toString().length()-1);
//						}else{
//							name = mName;
//							year="";
//						}
//						
//						//hbmu.addRecord(movieRatingJoinTable, rk.toString(), "movieName", "", name);
//						//hbmu.addRecord(movieRatingJoinTable, rk.toString(), "releaseYear", "", year);
//						
//						String genre = hbmu.getColumFamilyValue("movieTable", lineElems1.get(0).toString(), "genre");
//						//hbmu.addRecord(movieRatingJoinTable, rk.toString(), "genre", "", genre);
//						
//						rk+=1;
//					//	System.out.println("NAme:"+ mName+" genre"+ genre);
//					}
//					sc1.close();
//				}
				
//				rk = 0;
//				String fn = "/Users/aishwaryasrinivasan/Documents/Big_Data/Assignment3/ml-1m/movies.dat";
//				File textfile = new File(fn);
//				Scanner in = new Scanner(new FileInputStream(textfile));
//
//				while(in.hasNextLine()){
//					Movie movie = new Movie();
//					String line = in.nextLine();
//					StringTokenizer st = new StringTokenizer(line, "::");
//					ArrayList<String> lineElements =  new ArrayList<>();
//					while(st.hasMoreElements()){
//						lineElements.add(st.nextElement().toString());
//						
//					}
//					
//					String name;
//					String year;
//					
//					if(lineElements.get(1).contains("(")){
//						String[]elems =  lineElements.get(1).split(Pattern.quote("("));
//						 name =  elems[0].toString();
//						 year = elems[1].toString().substring(0, elems[1].toString().length()-1);
//					}else{
//						name = lineElements.get(1);
//						year="";
//					}
//					
//					
//					hbmu.addRecord(movieRatingJoinTable, rk.toString(), "movieName", "", name);
//					hbmu.addRecord(movieRatingJoinTable, rk.toString(), "releaseYear", "", year);
//					hbmu.addRecord(movieRatingJoinTable, rk.toString(), "genre", "", lineElements.get(2).toString());
//					rk+=1;
//					
//				}
//				
//				rk =0;
				
//				
//				String newFile = "/user/hduser/ml-1m/avgStars/part-r-00000";
//				File newTxtFile = new File(newFile);
//				Scanner newsc = new Scanner(newTxtFile);
//				while(newsc.hasNextLine()){
//					String newline = newsc.nextLine();
//					StringTokenizer newst = new StringTokenizer(newline,"\\t");
//					ArrayList<String> elems = new ArrayList<>();
//					while(newst.hasMoreElements()){
//						elems.add(newst.nextElement().toString());
//					}
//					hbmu.addRecord(movieRatingJoinTable, rk.toString(), "averageRating", "", elems.get(1));
//				}
//				
//				System.out.println("Movie table entered " +rk);
				
				
				
//				
//				}
//				catch(Exception e){
//					System.out.println(e);
//				}
//				
//		
//				
//				/*
//				 * Insert the sortedUserRating table into Hbase along with gender and age information 
//				 */
//				try{
//				String userRatingJoinTable = "userRatingJoinTable";
//				String[] familys = {"userId", "ratingCount", "gender", "age"};
//				HBaseManagementUtil.creatTable(userRatingJoinTable, familys);
//				HBaseManagementUtil hbmu = new HBaseManagementUtil();
//				int count=0;
//				String hdfsFileName = "hdfs://localhost:54310/user/hduser/ml-1m/topUsers/part-r-00000";
//				Path path = new Path(hdfsFileName);
//				Integer rowkey = 0;
//				FileSystem hdfsFS = FileSystem.get(conf);
//				if(hdfsFS.exists(path)){
//					FSDataInputStream ips = fileSystem.open(path);
//					Scanner sc = new Scanner(ips);
//					while(sc.hasNextLine()){
//						String line = sc.nextLine();
//						StringTokenizer st = new StringTokenizer(line, "\t");
//						ArrayList<String> lineElems = new ArrayList<>();
//						while(st.hasMoreElements()){
//							lineElems.add(st.nextElement().toString());
//						}
//						hbmu.addRecord(userRatingJoinTable, rowkey.toString(), "userId", "", lineElems.get(0).toString());
//						hbmu.addRecord(userRatingJoinTable, rowkey.toString(), "ratingCount", "", lineElems.get(1).toString());
//						rowkey+=1;
//					}
//				}
//				rowkey = 0;
//				String file = "/Users/aishwaryasrinivasan/Documents/Big_Data/Assignment3/ml-1m/users.dat";
//				File textfile = new File(file);
//				Scanner in = new Scanner(new FileInputStream(textfile));
//
//				while(in.hasNextLine()){
//					User user = new User();
//					String line = in.nextLine();
//					StringTokenizer st = new StringTokenizer(line, "::");
//					ArrayList<String> lineElements =  new ArrayList<>();
//					while(st.hasMoreElements()){
//						lineElements.add(st.nextElement().toString());
//						
//					}
//					hbmu.addRecord(userRatingJoinTable, rowkey.toString(),"gender", "", lineElements.get(1).toString());
//					hbmu.addRecord(userRatingJoinTable, rowkey.toString(), "age", "", lineElements.get(2));
//					rowkey+=1;
//					//count++;
//				}
//				}catch(Exception e){
//					System.out.println(e);
//				}
//				//System.out.println(count);
				
		}catch(Exception e){
			
			System.out.println(e);
		}
	}
	
	

}
