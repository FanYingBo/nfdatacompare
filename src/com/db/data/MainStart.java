package com.db.data;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import com.db.data.file.FileOperator;

/**
 * File export from Navicat MySql(.sql) of different environment
 * File Name Format: {business}-{env}.sql
 * @author Administrator
 *
 */
public class MainStart {
	
	private static String sqlFilePath = "C:\\Users\\Administrator\\Desktop\\3.0.0.5";
	private static String resultFilePath = "E:\\dbcompare\\result";
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("config/log4j.properties");
		String filePath = sqlFilePath;
		FileOperator fileOperator = new FileOperator(resultFilePath);
		try {
			fileOperator.execute(filePath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}