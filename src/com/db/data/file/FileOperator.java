package com.db.data.file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.db.data.cache.ModifyInfos;
import com.db.data.cache.TableDataHolder;
import com.db.data.common.Constants;


public class FileOperator {
	
	private static final Log logger = LogFactory.getLog(FileOperator.class);
	
	private TableDataHolder dataHolder;
	
	private boolean beCreate = Boolean.FALSE;
	
	private boolean beProcedure = Boolean.FALSE;
	
	private boolean beEvent = Boolean.FALSE;
	
	private String filePath;
	
	public FileOperator(String resultFilePath) {
		this.dataHolder = new TableDataHolder();
		this.filePath = resultFilePath;
	}
	
	
	public void execute(String filePath) throws IOException {
		preFileData(filePath);
		generateModifyInfo();
		writeModifyInfos();
	}
	
	public void preFileData(String filePath) throws IOException {
		checkFilePath(filePath, Boolean.FALSE);
		File[] files = getFiles(filePath);
		logger.info("---- 读取文件开始 ------");
		for(File sqlFile:files) {
			readFile(sqlFile);
		}
		logger.info("---- 读取文件结束 ------");
		if(logger.isDebugEnabled()) {
			dataHolder.printAll();
		}
	}
	
	private void checkFilePath(String filePath, Boolean createFilePath) throws IOException {
		File file = new File(filePath);
		if(!file.exists() && !createFilePath) {
			throw new IOException("文件目录【"+filePath+"】不存在");
		}
		if(!file.exists()) {
			file.mkdirs();
		}
	}


	public void generateModifyInfo() {
		logger.info("---- 生成修改信息开始 ------");
		dataHolder.generateModifyInfo();
		logger.info("---- 生成修改信息结束 ------");
		if(logger.isDebugEnabled()) {
			dataHolder.printModifyInfos();
		}
	}
	
	public void writeModifyInfos() throws IOException {
		logger.info("---- 写入修改信息开始 ------");
		Map<String, Map<String, List<ModifyInfos>>> allModifyInfos = dataHolder.getAllModifyInfos();
		Iterator<Entry<String, Map<String, List<ModifyInfos>>>> iterator = allModifyInfos.entrySet().iterator();
		while(iterator.hasNext()) {
			Entry<String, Map<String, List<ModifyInfos>>> entry = iterator.next();
			String environment = entry.getKey();
			Map<String, List<ModifyInfos>> map = entry.getValue();
			Iterator<Entry<String, List<ModifyInfos>>> modifyInfoIt = map.entrySet().iterator();
			while(modifyInfoIt.hasNext()) {
				Entry<String, List<ModifyInfos>> next = modifyInfoIt.next();
				String business = next.getKey();
				List<ModifyInfos> modifyInfos = next.getValue();
				writeResultFile(environment, business, modifyInfos);
			}
		}
		logger.info("---- 写入修改信息结束 ------");
	}
	
	private void writeResultFile(String environment,String business,List<ModifyInfos> modifyInfos) throws IOException {
		checkFilePath(filePath, Boolean.TRUE);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String formatstr = sdf.format(new Date());
		String fileName = business+"-"+environment+formatstr+".sql";
		File file = new File(filePath+File.separator+fileName);
		try {
			file.createNewFile();
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
			for(int index = 0;index < modifyInfos.size();index++) {
				ModifyInfos infos = modifyInfos.get(index);
				bw.write("/*\r\n");
				bw.write(infos.getAnnotaion());
				bw.newLine();
				bw.write("*/\r\n");
				bw.write(infos.getModifyInfo()+";");
				bw.newLine();
			}
			bw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	private File[] getFiles(String filePath) {
		File file = new File(filePath);
		File[] listFiles = file.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.getAbsolutePath().endsWith(".sql");
			}
		});
		return listFiles;
	}
	
	private void readFile(File sqlFile) {
		try {
			String name = sqlFile.getName();
			String[] split = name.split("-");
			String instanceName = split[0];
			String area = split[1].replace(".sql", "");
			dataHolder.saveDBInstance(area,instanceName);
			FileInputStream fis = new FileInputStream(sqlFile);
			InputStreamReader bsr = new InputStreamReader(fis);
			BufferedReader bfr = new BufferedReader(bsr);
			String str = null;
			while((str = bfr.readLine()) != null) {
				if(LineChecker.checkProcedure(str) && !LineChecker.checkDrop(str) && !LineChecker.checkComment(str)) {
					logger.debug("procedure line:"+str);
					beProcedure = Boolean.TRUE;
					saveProcedure(str);
				}
				if(LineChecker.checkEvent(str) && !LineChecker.checkDrop(str) && !LineChecker.checkComment(str)){
					logger.info("event line:"+str);
					beEvent = Boolean.TRUE;
				}
				if(LineChecker.checkCreate(beProcedure, beEvent, str)) {
					beCreate = Boolean.TRUE;
					saveTableName(str);
				} else if(LineChecker.checkInsertInto(beProcedure, beEvent, str)){
					saveColumnValue(str);
				} else {
					if(LineChecker.checkDelimiter(str)) {
						beProcedure = Boolean.FALSE;
						beEvent = Boolean.FALSE;
					}
					if(LineChecker.checkCreateEnd(str)) {
						beCreate = Boolean.FALSE;
						saveCreateTableEnd(str);
					}
					if(beCreate) {
						saveTableColumn(str);
					}
					if(beProcedure) {
						saveProcedureLine(str);
					}
					if(beEvent) {
						
					}
				}
			}
			fis.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	private void saveTableName(String line) {
		String tableName = line.substring(line.indexOf("`"),line.lastIndexOf("`")+1);
		dataHolder.saveTableName(tableName);
	}
	
	private void saveTableColumn(String line) {
		if(LineChecker.checkPrimaryKey(line)) {
			String tableColumn = line.substring(line.indexOf("(")+1, line.indexOf(")"));
			String[] keyColumns = tableColumn.split(",");
			for(String keyColumn : keyColumns) {
				dataHolder.saveKeyColumn(keyColumn.trim());
			}
			return;
		}
		String trim = line.trim();
		String column = "";
		if(trim.endsWith(",")) {
			column = trim.substring(0, trim.lastIndexOf(","));
		}else {
			column = trim;
		}
		if(!column.matches(Constants.reg_index)) {
			String columnName = column.substring(column.indexOf("`"), column.lastIndexOf("`") + 1);
			dataHolder.saveTableColumn(columnName);
			dataHolder.saveTableColumnDesc(columnName, column);
		}else {
			String indexName = column.substring(column.indexOf("`"), column.indexOf("("));
			dataHolder.saveTableIndex(indexName, column);
		}
	}
	
	private void saveCreateTableEnd(String createEnd) {
		dataHolder.saveCreateTableEnd(createEnd);
	}
	
	private void saveProcedure(String line) {
		String procedureName =line.substring(line.indexOf(Constants.PROCEDURE) + Constants.PROCEDURE.length() + 1,line.indexOf("("));
		dataHolder.saveProcedure(procedureName);
	}
	
	private void saveProcedureLine(String line) {
		dataHolder.saveProcedureLine(line);
	}
	
	private void saveEvent(String line) {
		String event = line.substring(line.indexOf(Constants.EVENT) + Constants.EVENT.length() + 1, line.lastIndexOf("`")+1);
	}
	
	private void saveColumnValue(String line) {
		String tableName = line.substring(line.indexOf("`"),line.lastIndexOf("VALUES")).trim();
		String values = line.substring(line.indexOf("(")+1,line.lastIndexOf(")"));
		char[] charArray = values.toCharArray();
		int beginIndex = 0;
		int endIndex = 0;
		boolean cutVarcharValue = Boolean.FALSE; 
		boolean cutNumberValue = Boolean.FALSE;
		boolean endCut = Boolean.FALSE;
		int countIndex = 0;
		for(int index = 0;index < charArray.length;index++) {
			char current = charArray[index];
			if((index == 0 && (current == '\'' || current == 'b')) || (index > 1 && current == '\'' && charArray[index - 2] == ',')
					||(index > 1 && current == 'b' && charArray[index - 2] == ',' && charArray[index + 1] == '\'')) {
				cutVarcharValue = Boolean.TRUE;
				beginIndex = index;
			}
			if(!cutVarcharValue) {
				if((index == 0 && current != '\'') || (index > 1 && current != '\''&& charArray[index - 2] == ',')) {
					cutNumberValue = Boolean.TRUE;
					beginIndex = index;
				}
			}
			if(cutVarcharValue && ',' == current && charArray[index - 1] == '\'') {
				endIndex = index;
				endCut = Boolean.TRUE;
			}
			if(cutNumberValue && !cutVarcharValue) {
				if(',' == current && charArray[index - 1] != '\'') {
					endIndex = index;
					endCut = Boolean.TRUE;
				}
			}
			if(index == charArray.length - 1) {
				endIndex = index + 1;
				endCut = Boolean.TRUE;
			}
			if(endCut) {
				String columnValue = values.substring(beginIndex, endIndex).trim();
				dataHolder.saveTableColumnValue(tableName,columnValue, countIndex);
				cutVarcharValue = Boolean.FALSE;
				cutNumberValue = Boolean.FALSE;
				endCut = Boolean.FALSE;
				countIndex ++;
			}
		}
	}
}
