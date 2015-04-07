package MovieLens;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

import javax.sound.sampled.LineUnavailableException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;




/*
 *@author Aishwarya Srinivasan
 */

public class MovieLens{

	/*
	 * This class reads data files stored in the hdfs file system
	 */
	public static void main(String[] args) {
		
		/*
		 * Reading movie.dat file using scanner
		 */
		try {
			String movieTable = "movieTable";
			String[] familys = {"movieNumber","movieName", "genre"};
			HBaseManagementUtil.creatTable(movieTable, familys);
			//HBaseManagementUtil.deleteTable(movieTable);
			HBaseManagementUtil hbmu = new HBaseManagementUtil();
			
			String filename = "/Users/aishwaryasrinivasan/Documents/Big_Data/Assignment3/ml-1m/movies.dat";
			File textfile = new File(filename);
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
				
				hbmu.addRecord(movieTable, movie.getMovieId(), "movieNumber", "", movie.getMovieId());
				hbmu.addRecord(movieTable, movie.getMovieId(), "movieName", "", movie.getMovieName());
				hbmu.addRecord(movieTable, movie.getMovieId(), "genre", "", movie.getGenre());
			}
			System.out.println("Movie table entered");
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
	
		/*
		 * Reading user.dat file using scanner
		 */
		try {
			String userTable = "userTable";
			String[] familys = {"userId","gender", "age", "occupation","zipcode"};
			HBaseManagementUtil.creatTable(userTable, familys);
			
			HBaseManagementUtil hbmu = new HBaseManagementUtil();
			/*
			 * Reading ratings.dat file using scanner
			 */
			String filename = "/Users/aishwaryasrinivasan/Documents/Big_Data/Assignment3/ml-1m/users.dat";
			File textfile = new File(filename);
			Scanner in = new Scanner(new FileInputStream(textfile));

			while(in.hasNextLine()){
				User user = new User();
				String line = in.nextLine();
				StringTokenizer st = new StringTokenizer(line, "::");
				ArrayList<String> lineElements =  new ArrayList<>();
				while(st.hasMoreElements()){
					lineElements.add(st.nextElement().toString());
					
				}
				user.setUserID(lineElements.get(0));
				user.setGender(lineElements.get(1));
				user.setAge(lineElements.get(2));
				user.setOccupation(lineElements.get(3));
				user.setZipcode(lineElements.get(4));
				
				hbmu.addRecord(userTable, user.getUserID(), "userId", "", user.getUserID());
				hbmu.addRecord(userTable, user.getUserID(), "gender", "", user.getGender());
				hbmu.addRecord(userTable, user.getUserID(), "age", "", user.getAge());
				hbmu.addRecord(userTable, user.getUserID(), "occupation", "", user.getOccupation());
				hbmu.addRecord(userTable, user.getUserID(), "zipcode", "", user.getOccupation());
			}
			System.out.println("User table entered");
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
		
		/*
		 * Read data from the hdfs output into hbase
		 */
		try{
			
			String userRatingTable = "userRatingTable";
			String familys[] = {"userId", "numberOfRatings"};
			HBaseManagementUtil.creatTable(userRatingTable, familys);
			HBaseManagementUtil hbmu = new HBaseManagementUtil();
			
			Configuration conf = new Configuration();
			conf.addResource(new Path("/usr/local/Cellar/hadoop/1.2.1/libexec/conf/core-site.xml"));
			conf.addResource(new Path("/usr/local/Cellar/hadoop/1.2.1/libexec/conf/hdfs-site.xml"));
			String filename = "hdfs://localhost:54310/user/hduser/ml-1m/usrrat/part-r-00000";
			Path p = new Path(filename);
			FileSystem fileSystem = FileSystem.get(conf);
			 if (fileSystem.exists(p)) {
				FSDataInputStream ips = fileSystem.open(p);
				Scanner sc = new Scanner(ips);
				int count = 0;
				System.out.println("exists");
				while(sc.hasNextLine()){
					String line = sc.nextLine();
					StringTokenizer st = new StringTokenizer(line, "\t");
					ArrayList<String> lineElements =  new ArrayList<>();
					while(st.hasMoreElements()){
						lineElements.add(st.nextElement().toString());
					}
					System.out.println(lineElements.get(0)+"and"+lineElements.get(1));
					hbmu.addRecord(userRatingTable, lineElements.get(0), "userId", "", lineElements.get(0));
					hbmu.addRecord(userRatingTable, lineElements.get(0), "numberOfRatings", "", lineElements.get(1));
				}
				System.out.println("entered into user table "+count+" values");	
			 }
			 
			
		}catch(Exception e){
			System.out.println(e);
		}
	}

}
