package com.db.data;

import org.apache.log4j.PropertyConfigurator;

import com.db.data.file.FileOperator;

/**
 * File export from Navicat MySql(.sql) of different environment
 * File Name Format: {business}-{env}.sql
 * @author Administrator
 *
 */
public class MainStart {
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("config/log4j.properties");
		String filePath = "C:\\Users\\Administrator\\Desktop\\3.0.0.5";
		String resultFilePath = "E:\\dbcompare";
		FileOperator fileOperator = new FileOperator(resultFilePath);
		fileOperator.execute(filePath);
	}
}