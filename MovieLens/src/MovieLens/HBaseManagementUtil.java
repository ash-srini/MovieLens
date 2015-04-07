package MovieLens;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/*
 *@author Aishwarya Srinivasan
 */

public class HBaseManagementUtil {

	private static Configuration conf = null;
	 
	    /**
	     * Initialization
	     */
	    static {
	        conf = HBaseConfiguration.create();
	    }
	 
	    /**
	     * Create a table
	     */
	    public static void creatTable(String tableName, String[] familys)
	            throws Exception {
	        HBaseAdmin admin = new HBaseAdmin(conf);
	        if (admin.tableExists(tableName)) {
	            System.out.println("table already exists!");
	        } else {
	            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
	            for (int i = 0; i < familys.length; i++) {
	                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
	            }
	            admin.createTable(tableDesc);
	            System.out.println("create table " + tableName + " ok.");
	        }
	    }
	 
	    /**
	     * Delete a table
	     */
	    public static void deleteTable(String tableName) throws Exception {
	        try {
	            HBaseAdmin admin = new HBaseAdmin(conf);
	            admin.disableTable(tableName);
	            admin.deleteTable(tableName);
	            System.out.println("delete table " + tableName + " ok.");
	        } catch (MasterNotRunningException e) {
	            e.printStackTrace();
	        } catch (ZooKeeperConnectionException e) {
	            e.printStackTrace();
	        }
	    }
	 
	    /**
	     * Put (or insert) a row
	     */
	    public static void addRecord(String tableName, String rowKey,
	            String family, String qualifier, String value) throws Exception {
	        try {
	            HTable table = new HTable(conf, tableName);
	            Put put = new Put(Bytes.toBytes(rowKey));
	            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
	                    .toBytes(value));
	            table.put(put);
	            //System.out.println("insert recored " + rowKey + " to table "+ tableName + " ok.");
	            table.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	    
	    /**
	     * Scan (or list) a table
	     */
	    public static void getAllRecord (String tableName) {
	        try{
	        	
	        	String fName = "/Users/hduser/tmp/ml-1m/movieInfo.csv";
	    		File f = new File(fName);
	        	
	    		FileWriter fileWritter = new FileWriter(fName,true);
	            PrintWriter pw = new PrintWriter(fileWritter);

	    		
	             HTable table = new HTable(conf, tableName);
	             Scan s = new Scan();
	             ResultScanner ss = table.getScanner(s);
	             for(Result r:ss){
	                 for(KeyValue kv : r.raw()){
	                	 
	                	 pw.write(new String(kv.getValue())+"\t");

	                 }
	                 pw.write("\n");
	             }
	        } catch (IOException e){
	            e.printStackTrace();
	        }
	    }
	    
	    
	    /*
	     * Gets value of a column from a particular hbase table for a particular rowkey 
	     */
	    public static String getColumFamilyValue(String tableName, String rowkey, String columnFam){
	    	try{
	    		
	    		HTable table = new HTable(conf, tableName);
	    	Get get = new Get(Bytes.toBytes(rowkey));
	    	Result r = table.get(get);
	    	
	    	byte[] b = r.getValue(Bytes.toBytes(columnFam), Bytes.toBytes(""));
	    	String s = null;
	    	for(KeyValue kv : r.raw()){
	    		 s = new String(kv.getValue());
	    	}
	    	if(s!=null)
	    	return (s);
	    	else
	    		return "no match";	
	    		
	    	}catch(Exception e){
	    		System.out.println(e);
	    	}
			return "no match";
	    }
	    
	    /*
	     * 
	     */
	   
	
}
