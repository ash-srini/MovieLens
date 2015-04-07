package Mahout;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

/*
 *@author Aishwarya Srinivasan
 */

public class CSVFileParser {

	public static void main(String[] args) {
	try{
		String fName = "/Users/hduser/tmp/ml-1m/ratings.dat";
		File f = new File(fName);
		
		String fileName = "/Users/hduser/tmp/ml-1m/ratings_csv.csv";
		File file = new File(fileName);
		if (!file.exists()) {
			file.createNewFile();
		}
		
		FileWriter fileWritter = new FileWriter(fileName,true);
        PrintWriter pw = new PrintWriter(fileWritter);

        Scanner sc = new Scanner(f);
        while(sc.hasNextLine()){
        	String line = sc.nextLine();
        	StringTokenizer st = new StringTokenizer(line, "::");
        	ArrayList<String> elems = new ArrayList<>();
        	while(st.hasMoreElements()){
        		elems.add(st.nextElement().toString());
        	}
        	if(!elems.get(0).equals(""))
        	pw.write(elems.get(0)+","+elems.get(1)+","+elems.get(2)+"\n");
        	
        }
        
        System.out.println("done");
	
	}catch(Exception e){
		System.out.println(e);
	}
}
}
