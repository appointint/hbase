package com.hbase.FilterTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;



public class Stu_Cour_Filter {
	static Configuration conf = HBaseConfiguration.create();
	static String stableName="student";
	static String ctableName="course";
	public static void main(String[] args) throws IOException {
		Stu_id_Cour_id_tit("s1");
		Cour_id_Ttu_id_name("c0");
		Tea_id_Cour_tit("teacher4");
		More_Class_Stu();
		Less_Class_stu();
		
	}
	//	根据学号student_id查询学生选课编号course_id和名称title
	public static void Stu_id_Cour_id_tit(String stuid) throws IOException{
		HTable stable=new HTable(conf, stableName);
		HTable ctable=new HTable(conf, ctableName);
		List<String> courseid=new ArrayList<String>();
		Scan scan=new Scan();
		System.out.println("scan student通过id查找..........................................................");
		Filter filter=new RowFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(stuid)));
		scan.setFilter(filter);
		ResultScanner resultScanner=stable.getScanner(scan);
		for (Result result : resultScanner) {
//			System.out.println(result);
			for (KeyValue keyValue : result.raw()) {  
				 
				  String courfamilier=new String(keyValue.getFamily());
//	               System.out.println(courfamilier);
	               if(courfamilier.equals("course")){
	            	   System.out.println("列：" + new String(keyValue.getFamily()) 
		                		+"===列限定符"+new String(keyValue.getQualifier())
		                        + "====值:" + new String(keyValue.getValue())); 
	                String courQualifier=new String(keyValue.getQualifier());
	                courseid.add(courQualifier);
	               }
			}
		}
		resultScanner.close();
		System.out.println("courseid 该学生选的课程有："+courseid);
		
		System.out.println("scan course 课程名称...........................................................");
		for (int i = 0; i < courseid.size(); i++) {
				System.out.println("课程号："+courseid.get(i));
				Filter filter2=new RowFilter(CompareFilter.CompareOp.EQUAL,
						new BinaryComparator(Bytes.toBytes(courseid.get(i))));
				scan.setFilter(filter2);
				ResultScanner resultScanner2=ctable.getScanner(scan);
				for (Result result : resultScanner2) {
//					System.out.println(result);
					for (KeyValue keyValue : result.raw()) {  
			               if(new String(keyValue.getQualifier()).equals("tittle")){
			            	   System.out.println("====值:" + new String(keyValue.getValue())); 
			               }
					}
				}
				resultScanner.close();
		}
	}
	
	//	根据课程号course_id查询选课学生学号student_id和姓名name
	public static void Cour_id_Ttu_id_name(String courid) throws IOException{
		HTable stable=new HTable(conf, stableName);
		HTable ctable=new HTable(conf, ctableName);
		List<String> studentid=new ArrayList<String>();
		Scan scan=new Scan();
		System.out.println("scan course通过id查找..........................................................");
		Filter filter=new RowFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(courid)));
		scan.setFilter(filter);
		ResultScanner resultScanner=ctable.getScanner(scan);
		for (Result result : resultScanner) {
//			System.out.println(result);
			for (KeyValue keyValue : result.raw()) {  
				 
				  String courfamilier=new String(keyValue.getFamily());
//	               System.out.println(courfamilier);
	               if(courfamilier.equals("student")){
	            	   System.out.println("列：" + new String(keyValue.getFamily()) 
		                		+"===列限定符"+new String(keyValue.getQualifier())
		                        + "====值:" + new String(keyValue.getValue())); 
	                String courQualifier=new String(keyValue.getQualifier());
	                studentid.add(courQualifier);
	               }
			}
		}
		resultScanner.close();
		System.out.println("courseid 该学生选的课程有："+studentid);
		
		System.out.println("scan course 课程名称...........................................................");
		for (int i = 0; i < studentid.size(); i++) {
				System.out.println("课程号："+studentid.get(i));
				Filter filter2=new RowFilter(CompareFilter.CompareOp.EQUAL,
						new BinaryComparator(Bytes.toBytes(studentid.get(i))));
				scan.setFilter(filter2);
				ResultScanner resultScanner2=stable.getScanner(scan);
				for (Result result : resultScanner2) {
//					System.out.println(result);
					for (KeyValue keyValue : result.raw()) {  
			               if(new String(keyValue.getQualifier()).equals("name")){
			            	   System.out.println("====值:" + new String(keyValue.getValue())); 
			               }
					}
				}
				resultScanner.close();
		}
	}
	
	
	//	根据教员号teacher_id查询该教员所上课程编号course_id和名称title
	@SuppressWarnings("deprecation")
	public static void Tea_id_Cour_tit(String teacherid) throws IOException{
//		HTable stable = new HTable(conf, stableName);
		HTable ctable = new HTable(conf, ctableName);

		List<String> plist = new ArrayList<String>();
		String teacher="";
		Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(teacherid));
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner resultScanner = ctable.getScanner(scan);
		for (Result result : resultScanner) {
			System.out.println("pro rowkey:" + new String(result.getRow()));
			teacher=new String(result.getRow());
			for (KeyValue keyValue : result.raw()) {
				System.out.println(
						"    列:" + new String(keyValue.getQualifier()) + ",值:" + new String(keyValue.getValue()));
			}
		}
		resultScanner.close();
		
		
		Filter filter2=new RowFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(teacher)));
		scan.setFilter(filter2);
		ResultScanner resultScanner2=ctable.getScanner(scan);
		for (Result result : resultScanner2) {
//			System.out.println(result);
			for (KeyValue keyValue : result.raw()) {  
	               if(new String(keyValue.getQualifier()).equals("tittle")){
	            	   System.out.println("====值:" + new String(keyValue.getValue())); 
	          
	               }
			}
		}
		resultScanner.close();
	}
	
	//	上课最多的学生
	@SuppressWarnings({ "unchecked", "unused" })
	public static void More_Class_Stu() throws IOException{
		HTable stable=new HTable(conf, stableName);
		HTable ctable=new HTable(conf, ctableName);
		List<String> studentidlist=new ArrayList<String>();
		int num=0;
		String studentid="";
		Scan scan = new Scan();
		ResultScanner resultScanner = stable.getScanner(scan);
		for (Result result : resultScanner) {
//			System.out.println(result);
            studentidlist.add(new String(result.getRow()));
            int tempnum=0;
			for (KeyValue keyValue : result.raw()) {  
				if(new String(keyValue.getFamily()).equals("course")){
					tempnum++;
				}
			}
			if(tempnum>num){
				num=tempnum;
			}
		}
		
		System.out.println("stuidlist:"+studentidlist);
		System.out.println("morenum:"+num);
		
		System.out.println("scan studernt通过id查找..........................................................");
		Map<String, Integer> coursecount= new HashMap<String,Integer>();
		for (String stuid : studentidlist) {
			Get get=new Get(Bytes.toBytes(stuid));
			get.addFamily(Bytes.toBytes("course"));
			Result result=stable.get(get);
			int count=0;
			for(KeyValue keyValue:result.raw()){
				count++;
			}
			coursecount.put(stuid, count);
		}
		
		for(Map.Entry<String, Integer> entry:coursecount.entrySet()){
			if(num==entry.getValue()){
				studentid=entry.getKey();
				System.out.println("上课最多的学生为："+studentid+",上了"+num+"门课。");
			}
		}
//		Filter filter=new RowFilter(CompareFilter.CompareOp.EQUAL,
//				new BinaryComparator(Bytes.toBytes(courid)));
//		scan.setFilter(filter);
//		ResultScanner resultScanner=ctable.getScanner(scan);
//		for (Result result : resultScanner) {
//			System.out.println(result);
//			for (KeyValue keyValue : result.raw()) {  
//				 
//				  String courfamilier=new String(keyValue.getFamily());
//	               System.out.println(courfamilier);
//	               if(courfamilier.equals("student")){
//	            	   System.out.println("列：" + new String(keyValue.getFamily()) 
//		                		+"===列限定符"+new String(keyValue.getQualifier())
//		                        + "====值:" + new String(keyValue.getValue())); 
//	                String courQualifier=new String(keyValue.getQualifier());
//	                studentid.add(courQualifier);
//	               }
//			}
//		}
//		resultScanner.close();
//		System.out.println("courseid 该学生选的课程有："+studentid);

	}
	
	
	//	上课最少的学生
	public static void Less_Class_stu() throws IOException{
		HTable stable=new HTable(conf, stableName);
		HTable ctable=new HTable(conf, ctableName);
		List<String> studentidlist=new ArrayList<String>();
		int num=0;
		String studentid="";
		Scan scan = new Scan();
		ResultScanner resultScanner = stable.getScanner(scan);
		for (Result result : resultScanner) {
//			System.out.println(result);
            studentidlist.add(new String(result.getRow()));
            int tempnum=0;
			for (KeyValue keyValue : result.raw()) {  
				if(new String(keyValue.getFamily()).equals("course")){
					tempnum++;
				}
			}
			num=tempnum;
		}	
		
		ResultScanner resultScanner2 = stable.getScanner(scan);
		for (Result result : resultScanner2) {
//			System.out.println(result);
//            studentidlist.add(new String(result.getRow()));
            int tempnum=0;
			for (KeyValue keyValue : result.raw()) {  
				if(new String(keyValue.getFamily()).equals("course")){
					tempnum++;
				}
			}
			if(tempnum<num){
				num=tempnum;
			}
		}
		System.out.println("stuidlist:"+studentidlist);
		System.out.println("lessnum:"+num);
		
		System.out.println("scan studernt通过id查找..........................................................");
		Map<String, Integer> coursecount= new HashMap<String,Integer>();
		for (String stuid : studentidlist) {
			Get get=new Get(Bytes.toBytes(stuid));
			get.addFamily(Bytes.toBytes("course"));
			Result result=stable.get(get);
			int count=0;
			for(KeyValue keyValue:result.raw()){
				count++;
			}
			coursecount.put(stuid, count);
		}	
		for(Map.Entry<String, Integer> entry:coursecount.entrySet()){
//			lessnum=entry.getValue();
			if(num==entry.getValue()){
//				number=entry.getValue();
				studentid=entry.getKey();
				System.out.println("上课最少的学生为："+studentid+",上了"+num+"门课。");
			}
		}
	
	}
	
}
