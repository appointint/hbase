package hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Student_Filter {

	static Configuration configuration = HBaseConfiguration.create();  
	public static void main(String[] args) throws IOException {
		
		createTable("student", "info", "course");
		createTable("course", "info", "student");
		int age=(int)( Math.random()*10+10);
		
		for(int i=0;i<10;i++){
			insertData("student", "s"+i, "info", "name", "name"+i);
			insertData("student", "s"+i, "info", "age", "20");
			insertData("student", "s"+i, "info", "sex", ""+age);
			
			for(int j=0;j<(int)( Math.random()*9+1);j++){
				int x=(int)( Math.random()*9+1);
				insertData("student", "s"+i, "course", "c"+x, "course"+x);
			}
		}		
		for(int k=0;k<10;k++){
			
			insertData("course", "c"+k, "info", "tittle", "tittle"+k);
			insertData("course", "c"+k, "info", "introduction", "intro"+k);
			insertData("course", "c"+k, "info", "techear", "teacher"+k);
			for(int j=0;j<(int)( Math.random()*9+1);j++){
				int x=(int)( Math.random()*9+1);
				insertData("course", "c"+k, "info", "student"+x, "student"+x);
			}
		}
	}
	public static void createTable(String tableName,String family1,String family2) {  
        System.out.println("start create table ......");  
        try {  
        	HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration); 
            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建  
                hBaseAdmin.disableTable(tableName);  
                hBaseAdmin.deleteTable(tableName);  
                System.out.println(tableName + " is exist,detele....");  
            }  
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);  
            tableDescriptor.addFamily(new HColumnDescriptor(family1));  
            tableDescriptor.addFamily(new HColumnDescriptor(family2));  
            hBaseAdmin.createTable(tableDescriptor);  
        } catch (MasterNotRunningException e) {  
            e.printStackTrace();  
        } catch (ZooKeeperConnectionException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        System.out.println("end create table ......");  
    }  
	@SuppressWarnings({ "deprecation", "resource" })
	public static void insertData(String tablename, String row, String columnFamily, String column, String data)
			throws IOException {
		HTable table = new HTable(configuration, tablename);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put'" + row + "','" + columnFamily + ":" + column + "','" + data + "'");
	}
}
