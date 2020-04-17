package com.db.data.file;

import com.db.data.common.Constants;

public class LineChecker {
	
	public static boolean checkComment(String line) {
		return line.indexOf("--") >= 0;
	}

	public static boolean checkProcedure(String line) {
		return line.toUpperCase().matches(Constants.reg_procedure);
	}
	
	public static boolean checkDrop(String line) {
		return line.toUpperCase().indexOf(Constants.DROP) >= 0;
	}
	public static boolean checkEvent(String line) {
		return line.toUpperCase().matches(Constants.reg_events);
	}
	
	public static boolean checkCreate(boolean beProcedure,boolean beEvent,String line) {
		return !(beProcedure || beEvent) && line.toUpperCase().indexOf(Constants.CREATE) >= 0;
	}
	
	public static boolean checkInsertInto(boolean beProcedure,boolean beEvent,String line) {
		return !(beProcedure || beEvent) && line.toUpperCase().indexOf(Constants.INSERT) >= 0;
	}
	
	public static boolean checkDelimiter(String line) {
		return line.indexOf(Constants.DELIMITER) >= 0;
	}
	
	public static boolean checkCreateEnd(String line) {
		return line.indexOf(Constants.CREATEEND) >= 0 || line.indexOf(Constants.CREATEEND_FEDERATED) >= 0;
	}
	
	public static boolean checkPrimaryKey(String line) {
		return line.indexOf(Constants.PRIMARY_KEY) >= 0;
	}
}
