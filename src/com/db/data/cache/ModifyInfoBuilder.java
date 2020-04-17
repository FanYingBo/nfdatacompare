package com.db.data.cache;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.db.data.common.Constants;


public class ModifyInfoBuilder {

	public static final String CREATE_TABLE_ANNOTATIONS = "CREATE TABLE";
	public static final String DROP_TABLE_ANNOTATIONS = "DROP TABLE";
	public static final String ADD_COLUMN_ANNOTATIONS = "ADD COLUMN";
	public static final String DROP_COLUMN_ANNOTATIONS = "DROP COLUMN";
	public static final String ADD_INDEX_ANNOTATIONS = "ADD TABLE INDEX";
	public static final String DROP_INDEX_ANNOTATIONS = "DROP TABLE INDEX";
	public static final String INSERT_ANNOTATIONS = "INSERT TABLE";
	public static final String DELETE_FROM_ANNOTATIONS = "DELETE DATA";
	public static final String DROP_PRIMARYKEY_ANNOTATIONS = "DROP PRIMARY KEY";
	public static final String ADD_PRIMARYKEY_ANNOTATIONS = "ADD PRIMARY KEY";
	public static final String DROP_PROCEDURE_ANNOTATIONS = "DROP PROCEURE";
	public static final String ADD_PROCEDURE_ANNOTATIONS = "MODIFY PROCEURE";
	public static final String MODIFY_TABLE_COLUMN_ANNOTATIONS = "MODIFY TABLE COLUMN";
	public static final String UPDATE_TABLE_ANNOTATIONS = "UPDATE TABLE";
	
	public static final String CREATE_TABLE_MODIFY = "CREATE TABLE";
	public static final String DROP_TABLE_MODIFY = "DROP TABLE IF EXISTS";
	public static final String ALTER_MODIFY = "ALTER TABLE";
	public static final String ALTER_ADD_COLUMN = "ADD COLUMN";
	public static final String ALTER_ADD = "ADD";
	public static final String ALTER_DROP_COLUMN = "DROP COLUMN";
	public static final String ALTER_DROP_INDEX = "DROP INDEX";
	public static final String INSERT_INTO = "INSERT INTO";
	public static final String DELETE_FROM = "DELETE FROM";
	public static final String DROP_PRIMARY_KEY = "DROP PRIMARY KEY";
	public static final String ADD_PRIMARY_KEY = "ADD PRIMARY KEY";
	public static final String DROP_PROCEDURE = "DROP PROCEDURE IF EXISTS";
	public static final String MODIFY_COLUMN = "MODIFY";
	public static final String WHERE = "WHERE";
	public static final String UPDATE = "UPDATE";
	
	public static ModifyInfos createModifyInfos(String annotaions,String modifyInfo) {
		return new ModifyInfos(annotaions,modifyInfo);
	}
	
	public static ModifyInfos createTableModifyInfos(String detail, String modifyInfo) {
		return createModifyInfos(CREATE_TABLE_ANNOTATIONS+" "+detail, modifyInfo);
	}
	
	public static ModifyInfos createDropTableModifyInfos(String detail, String modifyInfo) {
		return createModifyInfos(DROP_TABLE_ANNOTATIONS+" "+detail, modifyInfo);
	}
	
	public static ModifyInfos createAddColumnModifyInfos(String detail, String modifyInfo) {
		return createModifyInfos(ADD_COLUMN_ANNOTATIONS+" "+detail, modifyInfo);
	}
	
	public static ModifyInfos createDropColumnModifyInfos(String detail, String modifyInfo) {
		return createModifyInfos(DROP_COLUMN_ANNOTATIONS+" "+detail, modifyInfo);
	}
	
	public static ModifyInfos createAddTableIndexModifyInfos(String detail,String modifyInfo) {
		return createModifyInfos(ADD_INDEX_ANNOTATIONS+" " +detail, modifyInfo);
	}
	
	public static ModifyInfos createDropTableIndexModifyInfos(String detail,String modifyInfo) {
		return createModifyInfos(DROP_INDEX_ANNOTATIONS+" " +detail, modifyInfo);
	}
	
	public static ModifyInfos createInsertModifyInfos(String detail,String modifyInfo) {
		return createModifyInfos(INSERT_ANNOTATIONS+" " +detail, modifyInfo);
	}
	
	public static ModifyInfos createDeleteModifyInfos(String detail,String modifyInfo) {
		return createModifyInfos(DELETE_FROM_ANNOTATIONS+" " +detail, modifyInfo);
	}
	
	public static ModifyInfos createDropPrimaryKey(String tableName) {
		return createModifyInfos(DROP_PRIMARYKEY_ANNOTATIONS+" "+tableName, ALTER_MODIFY + " "+ tableName + DROP_PRIMARY_KEY);
	}
	
	public static ModifyInfos createAddPrimaryKey(String detail,String modifyInfo) {
		return createModifyInfos(ADD_PRIMARYKEY_ANNOTATIONS+" "+detail, modifyInfo);
	}
	
	public static ModifyInfos createDropProcedureModifyInfos(String procedureName) {
		String procedure = procedureName.substring(procedureName.indexOf("PROCEDURE")+"PROCEDURE ".length(),procedureName.indexOf("("));
		return createModifyInfos(DROP_PROCEDURE_ANNOTATIONS+" "+procedure, DROP_PROCEDURE+" "+procedure+";\r\n"+"delimiter ;;");
	}
	
	public static ModifyInfos createAddProcedureModifyInfos(String detail,String modifyInfo) {
		return createModifyInfos(ADD_PROCEDURE_ANNOTATIONS+" "+detail, modifyInfo);
	}
	
	public static ModifyInfos createUpdateModifyInfos(String detail,String modifyInfo) {
		return createModifyInfos(UPDATE_TABLE_ANNOTATIONS+" "+detail, modifyInfo);
	}
	public static ModifyInfos createTableModifyInfos(String tableName,Set<String> keyColumns, Set<String> columns,Set<String> tableIndexs,String createEnd) {
		StringBuilder sb = new StringBuilder(CREATE_TABLE_MODIFY);
		sb.append(" ").append(tableName)
			.append(" (\r\n");
		int count = 0;
		for(String column : columns) {
			sb.append("	").append(column);
			if(count != columns.size()-1 || keyColumns.size() > 0 || tableIndexs.size() > 0) {
				sb.append(",\r\n");
			}
			count ++;
		}
		count = 0;
		sb.append("	").append(Constants.PRIMARY_KEY).append("(");
		for(String keyColumn : keyColumns) {
			sb.append(keyColumn);
			if(count != keyColumns.size() - 1) {
				sb.append(",");
			}
			count ++;
		}
		sb.append(")");
		if(tableIndexs.size() > 0) {
			sb.append(",\r\n");
		}
		count = 0;
		for(String index : tableIndexs) {
			sb.append("	").append(index);
			if(count != tableIndexs.size() - 1) {
				sb.append(",\r\n");
			}
			count ++;
		}
		sb.append("\r\n").append(createEnd).append("\r\n");
		return createTableModifyInfos(tableName,sb.toString());
	}
	
	public static ModifyInfos createDropTableModifyInfos(String tableName) {
		return createDropTableModifyInfos(tableName,DROP_TABLE_MODIFY + " "+ tableName);
	}
	
	public static ModifyInfos createAddTableColumnModifyInfos(String tableName,List<String> columns) {
		StringBuilder modify = new StringBuilder(ALTER_MODIFY);
		StringBuilder annotation = new StringBuilder();
		modify.append(" ").append(tableName)
			.append(" ")
			.append(ALTER_ADD_COLUMN)
			.append(" (");
		for(int index = 0;index < columns.size();index ++) {
			modify.append(columns.get(index));
			annotation.append(" "+ tableName+"."+columns.get(index));
			if(index != columns.size()-1) {
				modify.append(",");
			}
		}
		modify.append(")");
		return createAddColumnModifyInfos(annotation.toString(),modify.toString());
	}
	
	public static ModifyInfos createDropTableColumnModifyInfos(String tableName,List<String> columns) {
		StringBuilder modify = new StringBuilder(ALTER_MODIFY);
		StringBuilder annotation = new StringBuilder();
		modify.append(" ").append(tableName).append(" ");
		for(int index = 0;index < columns.size();index++) {
			modify.append(ALTER_DROP_COLUMN).append(" ").append(columns.get(index).split(" ")[0]);
			annotation.append(" ").append(columns.get(index).split(" ")[0]);
			if(index != columns.size()-1) {
				modify.append(",");
			}
		}
		return createDropColumnModifyInfos(annotation.toString(),modify.toString());
	}
	
	public static ModifyInfos createAddTableIndexModifyInfos(String tableName, List<String> tableIndexs) {
		StringBuilder modify = new StringBuilder(ALTER_MODIFY);
		StringBuilder annotation = new StringBuilder();
		modify.append(" ").append(tableName);
		annotation.append(" ").append(tableName).append(" ");
		for(int index = 0;index < tableIndexs.size();index++) {
			modify.append(ALTER_ADD).append(" ").append(tableIndexs.get(index));
			annotation.append(" ").append(tableIndexs.get(index));
			if(index != tableIndexs.size()-1) {
				modify.append(",");
			}
		}
		return createAddTableIndexModifyInfos(annotation.toString(), modify.toString());
	}
	
	public static ModifyInfos createDropTableIndexModifyInfos(String tableName, List<String> tableIndexs) {
		StringBuilder modify = new StringBuilder(ALTER_MODIFY);
		StringBuilder annotation = new StringBuilder();
		modify.append(" ").append(tableName);
		annotation.append(" ").append(tableName).append(" ");
		for(int index = 0;index < tableIndexs.size();index++) {
			String allIndex = tableIndexs.get(index);
			String indexName = allIndex.trim().substring(allIndex.trim().indexOf("`"),allIndex.trim().indexOf("("));
			modify.append(ALTER_DROP_INDEX).append(" ").append(indexName);
			annotation.append(" ").append(tableIndexs.get(index));
			if(index != tableIndexs.size()-1) {
				modify.append(",");
			}
		}
		return createDropTableIndexModifyInfos(annotation.toString(), modify.toString());
	}
	
	public static ModifyInfos createInsertModifyInfos(String tableName, Map<String,LinkedList<String>> columnValues) {
		StringBuilder modify = new StringBuilder();
		StringBuilder annotation = new StringBuilder();
		LinkedList<Entry<String, LinkedList<String>>> columnList = new LinkedList<Entry<String, LinkedList<String>>>();
		columnList.addAll(columnValues.entrySet());
		StringBuilder columnBuilder = new StringBuilder();
		columnBuilder.append(INSERT_INTO).append(" ").append(tableName).append("(");
		for(int in = 0; in < columnList.size();in ++) {
			Entry<String, LinkedList<String>> entry = columnList.get(in);
			String key = entry.getKey();
			columnBuilder.append(key);
			if(in != columnList.size() - 1) {
				columnBuilder.append(", ");
			}
		}
		columnBuilder.append(") VALUES (");
		LinkedList<String> tmpDataList = columnList.get(0).getValue();
		for(int index = 0;index < tmpDataList.size();index ++) {
			modify.append(columnBuilder);
			for(int in = 0; in < columnList.size();in ++) {
				Entry<String, LinkedList<String>> entry = columnList.get(in);
				LinkedList<String> values = entry.getValue();
				String value = values.get(index);
				modify.append(value);
				if(in != columnList.size() - 1) {
					modify.append(", ");
				}
			}
			modify.append(")");
			if(index != tmpDataList.size() - 1) {
				modify.append(";\r\n");
			}
		}
		annotation.append(INSERT_ANNOTATIONS).append(" ").append(tableName);
		return createModifyInfos(annotation.toString(), modify.toString());
	}
	
	public static ModifyInfos createDeleteModifyInfos(String tableName) {
		return createDeleteModifyInfos(tableName,DELETE_FROM +" "+tableName);
	}
	public static ModifyInfos createDeleteModifyInfos(String tableName,Map<String,LinkedList<String>> deleteValues) {
		StringBuilder modify = new StringBuilder();
		Iterator<Entry<String, LinkedList<String>>> iterator = deleteValues.entrySet().iterator();
		LinkedList<String> keyValueList = null;
		while(iterator.hasNext()) {
			Entry<String, LinkedList<String>> entry = iterator.next();
			String key = entry.getKey();
			if(keyValueList == null) {
				keyValueList = deleteValues.get(key);
			}
		}
		for(int index = 0;index < keyValueList.size(); index ++) {
			modify.append(DELETE_FROM).append(" ").append(tableName).append(" ").append(WHERE);
			int count = 0;
			Iterator<Entry<String, LinkedList<String>>> it = deleteValues.entrySet().iterator();
			while(it.hasNext()) {
				Entry<String, LinkedList<String>> next = it.next();
				String key = next.getKey();
				LinkedList<String> values = next.getValue();
				String value = values.get(index);
				modify.append(" ").append(key).append(" = ").append(value);
				if(count != values.size() - 1) {
					modify.append(" ").append("AND ");
				}
				count ++;
			}
			if(index != keyValueList.size() -1) {
				modify.append(";\r\n");
			}
		}
		return createDeleteModifyInfos(tableName, modify.toString());
	}
	
	public static ModifyInfos createDeleteModifyInfos(String tableName , Set<String> keyColumns, Map<String,LinkedList<String>> deleteValues) {
		StringBuilder modify = new StringBuilder();
		if(keyColumns != null) {
			Iterator<String> keyIterator = keyColumns.iterator();
			LinkedList<String> keyValueList = null;
			while(keyIterator.hasNext()) {
				String key = keyIterator.next();
				if(keyValueList == null) {
					keyValueList = deleteValues.get(key);
				}
			}
			for(int index = 0;index < keyValueList.size();index ++) {
				modify.append(DELETE_FROM).append(" ").append(tableName).append(" ").append(WHERE);
				Iterator<String> iterator = keyColumns.iterator();
				int count = 0;
				while(iterator.hasNext()) {
					String key = iterator.next();
					LinkedList<String> linkedList = deleteValues.get(key);
					String string = linkedList.get(index);
					modify.append(" ").append(key).append(" = ").append(string);
					if(count != keyColumns.size() - 1) {
						modify.append(" ").append("AND ");
					}
					count ++;
				}
				if(index != keyValueList.size() -1) {
					modify.append(";\r\n");
				}
			}
			return createDeleteModifyInfos(tableName, modify.toString());
		}else {
			return createDeleteModifyInfos(tableName, deleteValues);
		}
	}
	
	public static ModifyInfos createUpdateModifyInfos(String tableName,Set<String> keyColumns, Map<String,LinkedList<String>> updateValues) {
		StringBuilder modify = new StringBuilder();
		LinkedList<String> keyList = new LinkedList<String>();
		keyList.addAll(updateValues.keySet());
		keyList.removeAll(keyColumns);
		LinkedList<String> dataList = updateValues.get(keyList.get(0));
		for(int index = 0;index < dataList.size();index++) {
			modify.append(UPDATE).append(" ").append(tableName).append(" ").append("SET ");
			for(int in = 0;in < keyList.size();in++) {
				String key = keyList.get(in);
				LinkedList<String> linkedList = updateValues.get(key);
				String value = linkedList.get(index);
				modify.append(key).append("=").append(value);
				if(in != keyList.size() - 1) {
					modify.append(", ");
				}
			}
			modify.append(" ").append(WHERE);
			int count = 0;
			for(String keyColumn : keyColumns) {
				modify.append(" ").append(keyColumn).append("=").append(updateValues.get(keyColumn).get(index));
				if(count != keyColumns.size() - 1) {
					modify.append(" AND");
				}
				count++;
			}
			if(index != dataList.size() - 1) {
				modify.append(";\r\n");
			}
		}
		return createUpdateModifyInfos(tableName, modify.toString());
	}
	
	public static ModifyInfos createAddPrimaryKey(String tableName,Set<String> keyColumns) {
		StringBuilder modify = new StringBuilder(ALTER_MODIFY);
		StringBuilder annotation = new StringBuilder();
		modify.append(" ").append(tableName)
			.append(" ").append(ADD_PRIMARY_KEY).append(" (");
		annotation.append(" ").append(tableName).append(" ");
		int count = 0;
		for(String keyColumn:keyColumns) {
			modify.append(keyColumn);
			annotation.append(keyColumn+" ");
			if(count != keyColumns.size() - 1){
				modify.append(",");
			}
			count++;
		}
		modify.append(") USING BTREE");
		return createAddPrimaryKey(annotation.toString(), modify.toString());
	}
	
	public static ModifyInfos createAddProcedureModifyInfos(String procedureName,List<String> procedure,Map<Integer,String> procedureDiff) {
		StringBuilder modify = new StringBuilder();
		modify.append(DROP_PROCEDURE).append(" ")
			.append(procedureName).append(";\r\n").append("delimiter ;;\r\n");
		for(int index = 0; index < procedure.size(); index++) {
			modify.append(procedure.get(index)).append("\r\n");
		}
		modify.append(";;\r\n").append("delimiter").append(";");
		StringBuilder annotation = new StringBuilder();
		annotation.append(procedureName).append("\r\n");
		if(procedureDiff != null) {
			Iterator<Entry<Integer, String>> iterator = procedureDiff.entrySet().iterator();
			while(iterator.hasNext()) {
				Entry<Integer, String> entry = iterator.next();
				annotation.append("line: ").append(entry.getKey()).append(" ").append(entry.getValue()).append("\r\n");
			}
		}
		return createAddProcedureModifyInfos(annotation.toString(), modify.toString());
	}
	
	public static ModifyInfos createModifyColumnModifyInfos(String tableName, Map<String,String> oldDesc, Map<String,String> newDesc) {
		StringBuilder modify = new StringBuilder(ALTER_MODIFY);
		StringBuilder annotation = new StringBuilder();
		annotation.append(MODIFY_TABLE_COLUMN_ANNOTATIONS);
		Iterator<Entry<String, String>> iterator = oldDesc.entrySet().iterator();
		int count = 0;
		while(iterator.hasNext()) {
			Entry<String, String> columnNameEn = iterator.next();
			String columnName = columnNameEn.getKey();
			String oldColumnDesc = columnNameEn.getValue();
			String newColumnDesc = newDesc.get(columnName);
			modify.append(" ").append(tableName).append(" ").append(MODIFY_COLUMN).append(" ").append(newColumnDesc);
			annotation.append("\r\n").append(oldColumnDesc).append("->").append(newColumnDesc);
			count++;
			if(count != oldDesc.size()) {
				modify.append(";\r\n");
			}
		}
		return createModifyInfos(annotation.toString(), modify.toString());
	}
}
