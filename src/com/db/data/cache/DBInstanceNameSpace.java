package com.db.data.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public class DBInstanceNameSpace {
	
	private String business;
	
	private Map<String,LinkedHashMap<String,LinkedList<String>>> tableColumns = new HashMap<String, LinkedHashMap<String,LinkedList<String>>>();
	
	private Map<String,LinkedHashMap<String, String>> columnDescs = new HashMap<String, LinkedHashMap<String,String>>();
	
	private Map<String,Map<String,String>> tableIndexs = new HashMap<String,Map<String,String>>();
	
	private Map<String,String> createTableEnds = new HashMap<String,String>();
	
	private Map<String,LinkedHashSet<String>> keyColumns = new HashMap<String,LinkedHashSet<String>>();
	
	private Map<String,LinkedList<String>> procedures = new HashMap<String,LinkedList<String>>();
	
	private Map<String,LinkedList<String>> events = new HashMap<String,LinkedList<String>>();
	
	private List<ModifyInfos> modifyInfos = new ArrayList<ModifyInfos>();
	
	public DBInstanceNameSpace(String business) {
		this.business = business;
	}

	public LinkedHashMap<String,LinkedList<String>> getTableColumns(String tableName){
		return tableColumns.get(tableName);
	}
	
	public LinkedHashMap<String,String> getTableColumnDescs(String tableName){
		return columnDescs.get(tableName);
	}
	
	public Map<String,String> getTableIndex(String tableName){
		return tableIndexs.get(tableName);
	}
	
	public Map<String,LinkedList<String>> getProcedures(){
		return procedures;
	}
	
	public LinkedList<String> getProcedure(String procedureName) {
		return procedures.get(procedureName);
	}
	
	public LinkedList<String> getEvent(String event){
		return events.get(event);
	}
	
	public List<ModifyInfos> getModifyInfos() {
		return modifyInfos;
	}
	
	public Map<String,LinkedHashMap<String,LinkedList<String>>> getTableColumns(){
		return tableColumns;
	}
	
	public Map<String,Map<String,String>> getTableIndex(){
		return tableIndexs;
	}
	
	public Map<String,LinkedList<String>> getEvents(){
		return events;
	}
	
	public Map<String,LinkedHashMap<String,String>> getColumnDescs(){
		return columnDescs;
	}
	
	public LinkedHashMap<String,String> getColumnDescs(String tableName){
		return columnDescs.get(tableName);
	}
	
	public void removeDDLColumn(String tableName) {
		tableColumns.remove(tableName);
		keyColumns.remove(tableName);
		tableIndexs.remove(tableName);
		columnDescs.remove(tableName);
	}
	
	public void addKeyColumn(String tableName,String keyColumn) {
		LinkedHashSet<String> keyColumnList = keyColumns.get(tableName);
		if(keyColumnList == null) {
			keyColumnList = new LinkedHashSet<String>();
			keyColumns.put(tableName, keyColumnList);
		}
		keyColumnList.add(keyColumn);
	}
	
	public LinkedHashSet<String> getKeyColumn(String tableName) {
		return keyColumns.get(tableName);
	}
	
	public Map<String,LinkedHashSet<String>> getKeyColumns(){
		return keyColumns;
	}
	
	public Set<String> getColumns(String tableName){
		return tableColumns.get(tableName).keySet();
	}
	
	public LinkedHashMap<String,LinkedList<String>> addAndGetTableName(String tableName){
		LinkedHashMap<String,LinkedList<String>> columns = getTableColumns(tableName);
		if(columns == null) {
			columns = new LinkedHashMap<String,LinkedList<String>>();
			tableColumns.put(tableName, columns);
		}
		return columns;
	}
	
	public Map<String,String> addAndGetTableColumnDescs(String tableName){
		LinkedHashMap<String, String> tableColumnDescs = getTableColumnDescs(tableName);
		if(tableColumnDescs == null) {
			tableColumnDescs = new LinkedHashMap<String, String>();
			columnDescs.put(tableName, tableColumnDescs);
		}
		return tableColumnDescs;
	}
	
	public LinkedHashMap<String,LinkedList<String>> addAndGetTableColumn(String tableName,String tableColumn){
		LinkedHashMap<String,LinkedList<String>> columns = addAndGetTableName(tableName);
		LinkedList<String> columnDatas = columns.get(tableColumn);
		if(columnDatas == null) {
			columnDatas = new LinkedList<String>();
			columns.put(tableColumn, columnDatas);
		}
		return columns;
	}
	
	public Map<String,String> addAnGetTableColumnDesc(String tableName,String tableColumn,String tableColumnDesc){
		Map<String, String> addAndGetTableColumnDescs = addAndGetTableColumnDescs(tableName);
		addAndGetTableColumnDescs.put(tableColumn, tableColumnDesc);
		return addAndGetTableColumnDescs;
	}
	
	public Map<String, String> addTableIndex(String tableName,String indexName, String tableIndex) {
		Map<String, String> tableIndexMap = getTableIndex(tableName);
		if(tableIndexMap == null) {
			tableIndexMap = new HashMap<String, String>();
			tableIndexs.put(tableName, tableIndexMap);
		}
		tableIndexMap.put(indexName, tableIndex);
		return tableIndexMap;
	}
	
	public Map<String,LinkedList<String>> addAndGetColumnValue(String tableName,String value,int index){
		Map<String, LinkedList<String>> columns = getTableColumns(tableName);
		if(columns == null) {
			return null;
		}
		List<String> columnList = new ArrayList<>(columns.keySet());
		String string = columnList.get(index);
		LinkedList<String> columnValues = columns.get(string);
		columnValues.add(value);
		return columns;
	}
	
	public LinkedList<String> addProcedure(String procedureName) {
		LinkedList<String> procedure = getProcedure(procedureName);
		if(procedure == null) {
			procedure = new LinkedList<String>();
			procedures.put(procedureName, procedure);
		}
		return procedure;
	}
	
	public LinkedList<String> addEvent(String event){
		LinkedList<String> eventList = getEvent(event);
		if(eventList == null) {
			eventList = new LinkedList<String>();
			events.put(event, eventList);
		}
		return eventList;
	}
	
	public void addCreateTableEnd(String tableName,String createEnd) {
		createTableEnds.put(tableName, createEnd);
	}
	
	public String getCreateTableEnd(String tableName) {
		return createTableEnds.get(tableName);
	}
	
	public List<String> addProcedureLine(String procedureName ,String line) {
		List<String> procedureLines = getProcedure(procedureName);
		if(procedureLines == null) {
			procedureLines = addProcedure(procedureName);
		}
		procedureLines.add(line);
		return procedureLines;
	}
	
	public void addModifyInfo(List<ModifyInfos> modifys) {
		modifyInfos.addAll(modifys);
	}
	
	public void addModifyInfo(ModifyInfos modifys) {
		modifyInfos.add(modifys);
	}
	
	public String getBusiness() {
		return business;
	}
	
	public void printTableColumnValue() {
		Iterator<Entry<String, LinkedHashMap<String, LinkedList<String>>>> iterator = tableColumns.entrySet().iterator();
		while(iterator.hasNext()) {
			Entry<String, LinkedHashMap<String, LinkedList<String>>> entry = iterator.next();
			String tableName = entry.getKey();
			System.out.print("--------- table "+tableName+" ---------\r\n");
			LinkedHashMap<String, LinkedList<String>> columnValues = entry.getValue();
			Set<String> keySet = columnValues.keySet();
			StringBuilder sbCu = new StringBuilder();
			String oneColumn = "";
			int count = 0;
			for(String column : keySet){
				sbCu.append("        "+column+"       ");
				count ++;
				oneColumn = column;
				if(count == keySet.size()) {
					sbCu.append("\r\n");
					count = 0;
				}
			}
			System.out.print(sbCu.toString());
			List<String> values = columnValues.get(oneColumn);
			for(int index = 0; index < values.size(); index ++) {
				StringBuilder sbv = new StringBuilder();
				for(String column : keySet) {
					List<String> list = columnValues.get(column);
					Object value = list.get(index);
					sbv.append("        "+value+"       ");
					count ++;
					if(count == keySet.size()) {
						sbv.append("\r\n");
						count = 0;
					}
				}
				System.out.print(sbv.toString());
			}
		}
	}
	
	public void printProcedure() {
		Iterator<Entry<String, LinkedList<String>>> iterator = procedures.entrySet().iterator();
		while(iterator.hasNext()) {
			Entry<String, LinkedList<String>> entry = iterator.next();
			String procedureName = entry.getKey();
			List<String> procedure = entry.getValue();
			System.out.print("--------- procedure "+procedureName+" ---------\r\n");
			StringBuilder sb = new StringBuilder();
			for(int index = 0;index < procedure.size(); index++){
				sb.append(procedure.get(index)+"\r\n");
			}
			System.out.print(sb.toString());
		}
	}
	
	public void printTableIndex() {
		Iterator<Entry<String, Map<String, String>>> iterator = tableIndexs.entrySet().iterator();
		while(iterator.hasNext()) {
			Entry<String, Map<String, String>> entry = iterator.next();
			String tableName = entry.getKey();
			System.out.print("--------- table "+tableName+" ---------\r\n");
			Map<String, String> map = entry.getValue();
			Iterator<Entry<String, String>> tableIndexIt = map.entrySet().iterator();
			while(tableIndexIt.hasNext()) {
				Entry<String, String> tableIndexEn = tableIndexIt.next();
				String key = tableIndexEn.getKey();
				String value = tableIndexEn.getValue();
				System.out.print("--------- indexName: "+key+" detail: "+value+"---------\r\n");
			}
		}
	}
	
	public void printEvent() {
		Iterator<Entry<String, LinkedList<String>>> iterator = events.entrySet().iterator();
		while(iterator.hasNext()) {
			Entry<String, LinkedList<String>> entry = iterator.next();
			String eventName = entry.getKey();
			System.out.print("--------- event "+eventName+" ---------\r\n");
			LinkedList<String> envents = entry.getValue();
			Iterator<String> eventIt = envents.iterator();
			StringBuilder sb = new StringBuilder();
			while(eventIt.hasNext()) {
				String next = eventIt.next();
				sb.append(next).append("\r\n");
			}
			System.out.println(sb.toString());
		}
	}
	
}
