package com.db.data.common;

public class Constants {
	public static final String STANDARD_AREA = "test";
	public static final String VIETNAM_AREA = "vn";
	public static final String XJP_AREA = "xjp";
	
	public static final String DROP = "DROP";
	public static final String CREATE = "CREATE TABLE";
	public static final String INSERT = "INSERT INTO";
	public static final String CREATEEND = ") ENGINE = InnoDB";
	public static final String CREATEEND_FEDERATED = ") ENGINE = FEDERATED";
	
	public static final String DELIMITER = ";;";
	public static final String PRIMARY_KEY = "PRIMARY KEY";
	public static final String FULLTEXT = "FULLTEXT";
	public static final String PROCEDURE = "PROCEDURE";
	public static final String EVENT = "EVENT";
	
	public static final String reg_index = "^.*(FULLTEXT|PRIMARY KEY|INDEX|UNIQUE).*$";
	public static final String reg_procedure = "^.*PROCEDURE.*$";
	public static final String reg_events = "^.*CREATE DEFINER = `root`@`192.168.1%` EVENT.*$";
}
