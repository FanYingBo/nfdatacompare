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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.db.data.common.Constants;
import com.db.data.util.Collections;


public class TableDataHolder {
	
	private static final Log logger = LogFactory.getLog(TableDataHolder.class);
	
	private static final String separator = "&&";
	
	private DBInstanceNameSpace currentDBInstance;
	
	private String currentTableName;
	
	private String currentProcedure;
	
	
	
	private Map<String,Map<String,DBInstanceNameSpace>> dbInstanceNameSpaceMap = new HashMap<String, Map<String,DBInstanceNameSpace>>();
	
	public Map<String,DBInstanceNameSpace> getAreaBusiness(String area) {
		return dbInstanceNameSpaceMap.get(area);
	}
	
	public DBInstanceNameSpace getDBInstanceNameSpace(String area,String business) {
		Map<String, DBInstanceNameSpace> areaBusiness = getAreaBusiness(area);
		if(areaBusiness != null) {
			return areaBusiness.get(business);
		}
		return null;
	}
	
	public DBInstanceNameSpace saveDBInstance(String area,String business) {
		Map<String, DBInstanceNameSpace> areaBusiness = getAreaBusiness(area);
		currentDBInstance = new DBInstanceNameSpace(business);
		if(areaBusiness != null) {
			areaBusiness.put(business, currentDBInstance);
		}else {
			areaBusiness = new HashMap<String,DBInstanceNameSpace>();
			areaBusiness.put(business, currentDBInstance);
			dbInstanceNameSpaceMap.put(area, areaBusiness);
		}
		return currentDBInstance;
	}

	
	public void saveTableName(String tableName) {
		if(checkNoneNullDBInstance()) {
			currentDBInstance.addAndGetTableName(tableName);
		}
		currentTableName = tableName;
	}
	
	public void saveTableColumn(String tableColumn) {
		if(checkNoneNullDBInstance()) {
			currentDBInstance.addAndGetTableColumn(getCurrentTableName(), tableColumn);
		}
	}
	
	public void saveTableColumnDesc(String tableColumn, String tableColumnDesc) {
		if(checkNoneNullDBInstance()) {
			currentDBInstance.addAnGetTableColumnDesc(getCurrentTableName(), tableColumn, tableColumnDesc);
		}
	}
	
	public void saveTableColumnValue(String tableName,String columnValue, int countIndex) {
		if(checkNoneNullDBInstance()) {
			currentDBInstance.addAndGetColumnValue(tableName, columnValue, countIndex);
		}
	}
	
	public void saveTableIndex(String indexName,String index) {
		if(checkNoneNullDBInstance()) {
			currentDBInstance.addTableIndex(getCurrentTableName(), indexName, index);
		}
	}
	
	public void saveKeyColumn(String keyColumn) {
		if(checkNoneNullDBInstance()) {
			currentDBInstance.addKeyColumn(getCurrentTableName(), keyColumn);
		}
	}
	
	public void saveProcedure(String procedureName){
		if(checkNoneNullDBInstance()) {
			currentDBInstance.addProcedure(procedureName);
			currentProcedure = procedureName;
		}
	}
	
	public void saveCreateTableEnd(String createTableEnd) {
		if(checkNoneNullDBInstance()) {
			currentDBInstance.addCreateTableEnd(getCurrentTableName(), createTableEnd);
		}
	}
	
	public void saveProcedureLine(String line) {
		if(checkNoneNullDBInstance()) {
			currentDBInstance.addProcedureLine(getCurrentProcedure(), line);
		}
	}
	
	public Set<String> getColumns(){
		if(checkNoneNullDBInstance()) {
			return currentDBInstance.getColumns(getCurrentTableName());
		}
		return null;
	}
	public void saveEvent(String line) {
		
	}
	public String getCurrentTableName() {
		return currentTableName;
	}
	
	public String getCurrentProcedure() {
		return currentProcedure;
	}
	
	private boolean checkNoneNullDBInstance() {
		return currentDBInstance != null;
	}
	
	
	public void printAll() {
		Iterator<Entry<String, Map<String, DBInstanceNameSpace>>> iterator = dbInstanceNameSpaceMap.entrySet().iterator();
		while(iterator.hasNext()) {
			Entry<String, Map<String, DBInstanceNameSpace>> entry = iterator.next();
			String key = entry.getKey();
			Map<String, DBInstanceNameSpace> dbInstanceNameSpaces = entry.getValue();
			System.out.print("---area "+key +" ------------\r\n");
			Iterator<Entry<String, DBInstanceNameSpace>> dbinstanceIt = dbInstanceNameSpaces.entrySet().iterator();
			while(dbinstanceIt.hasNext()) {
				Entry<String, DBInstanceNameSpace> dbentry = dbinstanceIt.next();
				String business = dbentry.getKey();
				System.out.print("-----business "+business +" ------------\r\n");
				DBInstanceNameSpace dbInstanceNameSpace = dbentry.getValue();
//				dbInstanceNameSpace.printTableColumnValue();
//				dbInstanceNameSpace.printTableIndex();
//				dbInstanceNameSpace.printProcedure();
				dbInstanceNameSpace.printEvent();
			}
		}
		
	}
	
	public void printModifyInfos() {
		
	}
	
	public void generateModifyInfo() {
		Map<String, DBInstanceNameSpace> testDBMap = dbInstanceNameSpaceMap.get(Constants.STANDARD_AREA);
		Map<String, DBInstanceNameSpace> vietnamMap = dbInstanceNameSpaceMap.get(Constants.VIETNAM_AREA);
		Map<String, DBInstanceNameSpace> xjpMap = dbInstanceNameSpaceMap.get(Constants.XJP_AREA);
		if(testDBMap != null) {
			Iterator<Entry<String, DBInstanceNameSpace>> iterator = testDBMap.entrySet().iterator();
			while(iterator.hasNext()) {
				Entry<String, DBInstanceNameSpace> entry = iterator.next();
				String business = entry.getKey();
				DBInstanceNameSpace testDbInstanceNameSpace = entry.getValue();
				if(vietnamMap != null) {
					compareDBInstance(business,testDbInstanceNameSpace,vietnamMap);
				}
				if(xjpMap != null) {
					compareDBInstance(business,testDbInstanceNameSpace,xjpMap);
				}
			}
		}
	}
	
	public  Map<String,Map<String,List<ModifyInfos>>> getAllModifyInfos(){
		Iterator<Entry<String, Map<String, DBInstanceNameSpace>>> dbMapIt = dbInstanceNameSpaceMap.entrySet().iterator();
		Map<String,Map<String,List<ModifyInfos>>> moifyInfosMap = new HashMap<String, Map<String,List<ModifyInfos>>>();
		while(dbMapIt.hasNext()) {
			Entry<String, Map<String, DBInstanceNameSpace>> entry = dbMapIt.next();
			String key = entry.getKey();
			if(Constants.STANDARD_AREA.equals(key)) {
				continue;
			}
			Map<String, DBInstanceNameSpace> dbInstances = entry.getValue();
			Iterator<Entry<String, DBInstanceNameSpace>> dbInstanceIt = dbInstances.entrySet().iterator();
			Map<String,List<ModifyInfos>> modifyMap = new HashMap<String,List<ModifyInfos>>();
			while(dbInstanceIt.hasNext()) {
				Entry<String, DBInstanceNameSpace> dbInstEntry = dbInstanceIt.next();
				String dbName = dbInstEntry.getKey();
				DBInstanceNameSpace dbInstanceNameSpace = dbInstEntry.getValue();
				modifyMap.put(dbName, dbInstanceNameSpace.getModifyInfos());
			}
			moifyInfosMap.put(key, modifyMap);
		}
		return moifyInfosMap;
	}
	

	private void compareDBInstance(String business, DBInstanceNameSpace testDBInstanceNameSpace, Map<String, DBInstanceNameSpace> dbInstanceMap) {
		DBInstanceNameSpace dbInstanceNameSpace = dbInstanceMap.get(business);
		// 对比表
		List<ModifyInfos> tableModifyInfo = compareTable(testDBInstanceNameSpace, dbInstanceNameSpace);
		// 对比列
		List<ModifyInfos> tableColumnModifyInfo = compareTableColumn(testDBInstanceNameSpace, dbInstanceNameSpace);
		// 对比主键
		List<ModifyInfos> tablePrimaryKeyModify = compareTablePrimaryKey(testDBInstanceNameSpace, dbInstanceNameSpace);
		// 对比索引
		List<ModifyInfos> tableIndexModifyInfo = compareTableIndex(testDBInstanceNameSpace,dbInstanceNameSpace);
		// 对比值
		List<ModifyInfos> columnValueModifyInfo = compareColumnValue(testDBInstanceNameSpace, dbInstanceNameSpace);
		// 对比存储过程
		List<ModifyInfos> procedureModifyInfos = compareProcedure(testDBInstanceNameSpace,dbInstanceNameSpace);
		// 对比事件
		List<ModifyInfos> eventModifyInfos = compareEvent(testDBInstanceNameSpace,dbInstanceNameSpace);
		
		dbInstanceNameSpace.addModifyInfo(tableModifyInfo);
		dbInstanceNameSpace.addModifyInfo(tableColumnModifyInfo);
		dbInstanceNameSpace.addModifyInfo(tablePrimaryKeyModify);
		dbInstanceNameSpace.addModifyInfo(tableIndexModifyInfo);
		dbInstanceNameSpace.addModifyInfo(columnValueModifyInfo);
		dbInstanceNameSpace.addModifyInfo(procedureModifyInfos);
		dbInstanceNameSpace.addModifyInfo(eventModifyInfos);
	}
	

	private List<ModifyInfos> compareTable(DBInstanceNameSpace testDBInstanceNameSpace ,DBInstanceNameSpace dbInstanceNameSpace) {
		List<ModifyInfos> modifys = new ArrayList<ModifyInfos>(); 
		Map<String, LinkedHashMap<String, LinkedList<String>>> testTableColumns = testDBInstanceNameSpace.getTableColumns();
		Map<String, LinkedHashMap<String, LinkedList<String>>> tableColumns = dbInstanceNameSpace.getTableColumns();
		List<String> tableNameList = new ArrayList<>(testTableColumns.keySet());
		List<String> tableNameList_ = new ArrayList<>(tableColumns.keySet());
		List<String> complement = Collections.complement(tableNameList, tableNameList_);
		for(int index = 0;index < complement.size();index ++) {
			String tableNameStr = complement.get(index);
			if(testTableColumns.containsKey(tableNameStr)) { // add 
				
				Map<String, String> tableIndexMap = testDBInstanceNameSpace.getTableIndex(tableNameStr);
				LinkedHashMap<String, LinkedList<String>> columnDatas = testDBInstanceNameSpace.getTableColumns(tableNameStr);
				LinkedHashMap<String, String> tableColumnsCache = testDBInstanceNameSpace.getColumnDescs(tableNameStr);
				LinkedHashSet<String> testKeyColumns = testDBInstanceNameSpace.getKeyColumn(tableNameStr);
				
				ArrayList<String> columnList = new ArrayList<>(tableColumnsCache.keySet());
				
				LinkedHashSet<String> keyColumns = new LinkedHashSet<String>();
				keyColumns.addAll(testKeyColumns);
				LinkedHashSet<String> columnDescs = new LinkedHashSet<String>();
				columnDescs.addAll(tableColumnsCache.values());
				
				LinkedHashSet<String> tableIndexs = new LinkedHashSet<String>();
				if(tableIndexMap != null) {
					tableIndexs.addAll(tableIndexMap.values());
				}
				ModifyInfos tableModifyInfos = ModifyInfoBuilder.createTableModifyInfos(tableNameStr, keyColumns, columnDescs, tableIndexs, testDBInstanceNameSpace.getCreateTableEnd(tableNameStr));
				modifys.add(tableModifyInfos);
				if(columnDatas.get(columnList.get(0)).size() > 0) { // insert
					ModifyInfos insertModifyInfos = ModifyInfoBuilder.createInsertModifyInfos(tableNameStr, columnDatas);
					modifys.add(insertModifyInfos);
				}
				// 对新增表，删除test中的列，索引信息，防止在数据对比时再次对比
				testDBInstanceNameSpace.removeDDLColumn(tableNameStr); // delete ready
				System.out.println("delete table DDL: "+tableNameStr);
			}
			if(tableColumns.containsKey(tableNameStr)) { // drop
				ModifyInfos tableModifyInfos = ModifyInfoBuilder.createDropTableModifyInfos(tableNameStr);
				modifys.add(tableModifyInfos);
			}
		}
		return modifys;
	}
	
	private List<ModifyInfos> compareTableColumn(DBInstanceNameSpace testDBInstanceNameSpace, DBInstanceNameSpace dbInstanceNameSpace) {
		List<ModifyInfos> modifyInfos = new ArrayList<ModifyInfos>();
		Map<String, LinkedHashMap<String, String>> testColumnDescs = testDBInstanceNameSpace.getColumnDescs();
		Map<String, LinkedHashMap<String, String>> columnDescs = dbInstanceNameSpace.getColumnDescs();
		
		Iterator<Entry<String, LinkedHashMap<String, String>>> testTableColumnsIt = testColumnDescs.entrySet().iterator();
		while(testTableColumnsIt.hasNext()) {
			Entry<String, LinkedHashMap<String, String>> testEntry = testTableColumnsIt.next();
			String tableName = testEntry.getKey();
			Map<String, String> testColumnMap = testEntry.getValue();
			Map<String, String> columnMap = columnDescs.get(tableName);
			List<String> complement = Collections.complement(testColumnMap.keySet(), columnMap.keySet());
			List<String> dropColumn = new ArrayList<String>();
			List<String> addColumn = new ArrayList<String>();
			
			for(int index = 0;index < complement.size();index ++) {
				String columnName = complement.get(index);
				if(testColumnMap.containsKey(columnName)) { // add
					addColumn.add(testColumnMap.get(columnName));
				}
				if(columnMap.containsKey(columnName)) { // drop
					dropColumn.add(columnName);
				}
				
			}
			List<String> intersect = Collections.intersect(testColumnMap.keySet(), columnMap.keySet());
			List<String> modifyColumn = new ArrayList<String>();
			Map<String, String> testModifyColumnMap = new HashMap<String, String>();
			Map<String, String> modifyColumnMap = new HashMap<String, String>();
			
			for(int index = 0;index < intersect.size();index ++) {
				String testColumn = intersect.get(index);
				String testColumnDesc = testColumnMap.get(testColumn);
				String columnDesc = columnMap.get(testColumn);
				if(!testColumnDesc.equals(columnDesc)) {
					modifyColumn.add(testColumnDesc);
					testModifyColumnMap.put(testColumn, testColumnDesc);
					modifyColumnMap.put(testColumn, columnDesc);
				}
			}
			if(dropColumn.size() > 0) { // drop
				ModifyInfos dropTableColumnModifyInfos = ModifyInfoBuilder.createDropTableColumnModifyInfos(tableName, dropColumn);
				modifyInfos.add(dropTableColumnModifyInfos);
			}
			if(addColumn.size() > 0) { //  add
				ModifyInfos addTableColumnModifyInfos = ModifyInfoBuilder.createAddTableColumnModifyInfos(tableName, addColumn);
				modifyInfos.add(addTableColumnModifyInfos);
			}
			if(modifyColumn.size() > 0) { // modify
				ModifyInfos modifyColumnModifyInfos = ModifyInfoBuilder.createModifyColumnModifyInfos(tableName, modifyColumnMap, testModifyColumnMap);
				modifyInfos.add(modifyColumnModifyInfos);
			}
		}
		return modifyInfos;
	}
	
	private List<ModifyInfos> compareTablePrimaryKey(DBInstanceNameSpace testDBInstanceNameSpace, DBInstanceNameSpace dbInstanceNameSpace){
		List<ModifyInfos> moifyInfos = new ArrayList<ModifyInfos>();
		//设置主键之前先把列对比
		Map<String, LinkedHashSet<String>> testKeyColumns = testDBInstanceNameSpace.getKeyColumns();
		Iterator<Entry<String, LinkedHashSet<String>>> testKeyColumnsIt = testKeyColumns.entrySet().iterator();
		while(testKeyColumnsIt.hasNext()) {
			Entry<String, LinkedHashSet<String>> entry = testKeyColumnsIt.next();
			String tableName = entry.getKey();
			LinkedHashSet<String> testKeyColumnList = entry.getValue();
			LinkedHashSet<String> keyColumnList = dbInstanceNameSpace.getKeyColumn(tableName);
			if(!Collections.compareCollection(testKeyColumnList, keyColumnList)) {
				// 删除主键
				ModifyInfos dropPrimaryKey = ModifyInfoBuilder.createDropPrimaryKey(tableName);
				moifyInfos.add(dropPrimaryKey);
				// 添加主键
				ModifyInfos addPrimaryKey = ModifyInfoBuilder.createAddPrimaryKey(tableName, testKeyColumnList);
				moifyInfos.add(addPrimaryKey);
			}
		}
		return moifyInfos;
	}
	
	private List<ModifyInfos> compareTableIndex(DBInstanceNameSpace testDBInstanceNameSpace, DBInstanceNameSpace dbInstanceNameSpace) {
		List<ModifyInfos> moifyInfos = new ArrayList<ModifyInfos>();
		Map<String, Map<String, String>> testTableIndexs = testDBInstanceNameSpace.getTableIndex();
		Map<String, Map<String, String>> tableIndexs = dbInstanceNameSpace.getTableIndex();
		Iterator<Entry<String, Map<String, String>>> testTableIt = testTableIndexs.entrySet().iterator();
		while(testTableIt.hasNext()) {
			Entry<String, Map<String, String>> entry = testTableIt.next();
			String tableName = entry.getKey();
			Map<String, String> testIndexMap = entry.getValue();
			Map<String, String> indexMap = tableIndexs.get(tableName);
			Set<String> keySet = testIndexMap.keySet();
			Set<String> keySet_ = indexMap.keySet();
			List<String> addTableIndex = new ArrayList<String>();
			List<String> dropTableIndex = new ArrayList<String>();
			if(!Collections.compareCollection(keySet, keySet_)) {
				List<String> complementary = Collections.complement(keySet, keySet_); // 索引名称
				for(String index : complementary) {
					if(keySet.contains(index)) { // add
						addTableIndex.add(testIndexMap.get(index));
					}
					if(keySet_.contains(index)) {// drop
						dropTableIndex.add(index);
					}
				}
			}
			List<String> allHave = Collections.intersect(keySet, keySet_);
			for(int index = 0;index < allHave.size();index ++) {
				String indexName = allHave.get(index);
				String testIndexColumns = testIndexMap.get(indexName);
				String indexColumns = indexMap.get(indexName);
				if(!testIndexColumns.equals(indexColumns)) {
					dropTableIndex.add(indexName);
					addTableIndex.add(testIndexColumns);
				}
			}
			if(dropTableIndex.size() > 0) {
				ModifyInfos dropTableIndexModifyInfos = ModifyInfoBuilder.createDropTableIndexModifyInfos(tableName, dropTableIndex);
				moifyInfos.add(dropTableIndexModifyInfos);
			}
			if(addTableIndex.size() > 0) {
				ModifyInfos addTableIndexModifyInfos = ModifyInfoBuilder.createAddTableIndexModifyInfos(tableName, addTableIndex);
				moifyInfos.add(addTableIndexModifyInfos);
			}
			
		}
		return moifyInfos;
	}
	
	private List<ModifyInfos> compareColumnValue(DBInstanceNameSpace testDBInstanceNameSpace, DBInstanceNameSpace dbInstanceNameSpace){
		List<ModifyInfos> modifyInfos = new ArrayList<ModifyInfos>();
		Map<String, LinkedHashMap<String, LinkedList<String>>> testTableColumns = testDBInstanceNameSpace.getTableColumns();
		Map<String, LinkedHashMap<String, LinkedList<String>>> tableColumns = dbInstanceNameSpace.getTableColumns();
		Map<String, LinkedHashSet<String>> testKeyColumns = testDBInstanceNameSpace.getKeyColumns();
		Map<String, LinkedHashSet<String>> keyColumns = dbInstanceNameSpace.getKeyColumns();
		
		
		// 有主键 且 主键未变更的 建索引缓存
		Map<String,LinkedHashMap<String,String>> testTmpLinkedMap = new LinkedHashMap<String, LinkedHashMap<String,String>>();
		Map<String,LinkedHashMap<String,String>> tmpLinkedMap = new LinkedHashMap<String, LinkedHashMap<String,String>>();
		// 无主键
		List<String> tableList = new ArrayList<String>();
		Iterator<Entry<String, LinkedHashMap<String, LinkedList<String>>>> testTableColumnIt = testTableColumns.entrySet().iterator();
		while(testTableColumnIt.hasNext()) {
			Entry<String, LinkedHashMap<String, LinkedList<String>>> testTableColumnEntry = testTableColumnIt.next();
			String tableName = testTableColumnEntry.getKey();
			LinkedHashMap<String, LinkedList<String>> testColumnDataMap = testTableColumnEntry.getValue();
			LinkedHashMap<String, LinkedList<String>> columnDataMap = tableColumns.get(tableName);
			LinkedHashSet<String> testKeyColumnSet = testKeyColumns.get(tableName);
			LinkedHashSet<String> keyColumnSet = keyColumns.get(tableName);
			
			if(testKeyColumnSet != null && keyColumnSet != null) {
				if(Collections.compareCollection(testKeyColumnSet, keyColumnSet)) { // 有主键且主键一致的对主键建索引
					Iterator<String> iterator = testKeyColumnSet.iterator();
					LinkedList<String> tmpTestDataList = null;
					LinkedList<String> tmpDataList = null;
					while(iterator.hasNext()) {
						String keyColumn = iterator.next();
						if(tmpTestDataList == null) {
							tmpTestDataList = testColumnDataMap.get(keyColumn);
						}
						if(tmpDataList == null) {
							tmpDataList = columnDataMap.get(keyColumn);
						}
					}
					LinkedHashMap<String,String> testKeyDataIndexMap = new LinkedHashMap<String, String>();  
					LinkedHashMap<String,String> keyDataIndexMap = new LinkedHashMap<String, String>();  
					for(int index= 0;index < tmpTestDataList.size();index++){
						StringBuilder testKeyBuilder = new StringBuilder();
						StringBuilder keyBuilder = new StringBuilder();
						StringBuilder testDataBuilder = new StringBuilder();
						StringBuilder dataBuilder = new StringBuilder();
						Iterator<String> tmpIt = testKeyColumnSet.iterator();
						int count = 0;
						while(tmpIt.hasNext()) {
							String keyColumn = tmpIt.next();
							LinkedList<String> testDataList = testColumnDataMap.get(keyColumn);
							testKeyBuilder.append(testDataList.get(index));
							if(count  != testKeyColumnSet.size() - 1) {
								testKeyBuilder.append(separator);
							}
							LinkedList<String> dataList = columnDataMap.get(keyColumn);
							if(index < dataList.size()) {
								keyBuilder.append(dataList.get(index));
								if(count != keyColumnSet.size() - 1) {
									keyBuilder.append(separator);
								}
							}
							count++;
						}
						Iterator<Entry<String, LinkedList<String>>> testDataIt = testColumnDataMap.entrySet().iterator();
						while(testDataIt.hasNext()) {
							Entry<String, LinkedList<String>> next = testDataIt.next();
							LinkedList<String> linkedList = next.getValue();
							if(index < linkedList.size()) {
								testDataBuilder.append(linkedList.get(index)).append(separator);
							}
						}
						Iterator<Entry<String, LinkedList<String>>> dataIt = columnDataMap.entrySet().iterator();
						while(dataIt.hasNext()) {
							Entry<String, LinkedList<String>> next = dataIt.next();
							LinkedList<String> linkedList = next.getValue();
							if(index < linkedList.size()) {
								dataBuilder.append(linkedList.get(index)).append(separator);
							}
						}
						testKeyDataIndexMap.put(testKeyBuilder.toString(), testDataBuilder.substring(0, testDataBuilder.lastIndexOf(separator)));
						if(dataBuilder.length() > 0) {
							keyDataIndexMap.put(keyBuilder.toString(), dataBuilder.substring(0, dataBuilder.lastIndexOf(separator)));
						}
					}
					if(!testKeyDataIndexMap.isEmpty() || !keyDataIndexMap.isEmpty()) {
						
						testTmpLinkedMap.put(tableName, testKeyDataIndexMap);
						tmpLinkedMap.put(tableName, keyDataIndexMap);
					}
				} else {// 主键不一致
					tableList.add(tableName);
				}
			}else {// 无主键
				tableList.add(tableName);
			}
		}
		Iterator<Entry<String, LinkedHashMap<String, String>>> testKeyIndexValueIt = testTmpLinkedMap.entrySet().iterator();
		while(testKeyIndexValueIt.hasNext()) {
			Entry<String, LinkedHashMap<String, String>> entry = testKeyIndexValueIt.next();
			String tableName = entry.getKey();
			LinkedHashSet<String> linkedHashSet = testKeyColumns.get(tableName);
			LinkedHashMap<String, LinkedList<String>> testColumnsMap = testTableColumns.get(tableName);
			LinkedHashMap<String, String> testKeyIndexMap = entry.getValue();
			LinkedHashMap<String, String> keyIndexMap = tmpLinkedMap.get(tableName);
			List<String> intersect = Collections.intersect(testKeyIndexMap.keySet(), keyIndexMap.keySet());
			List<String> complement = Collections.complement(testKeyIndexMap.keySet(), keyIndexMap.keySet());
			LinkedHashMap<String,LinkedList<String>> updateMap = new LinkedHashMap<String, LinkedList<String>>(); // update
			LinkedHashMap<String, LinkedList<String>> insertMap = new LinkedHashMap<String, LinkedList<String>>(); // insert
			LinkedHashMap<String, LinkedList<String>> deleteMap = new LinkedHashMap<String, LinkedList<String>>(); // delete
			LinkedList<String> columnList = new LinkedList<String>();
			columnList.addAll(testColumnsMap.keySet());
			for(int out = 0; out < columnList.size();out++) {
				String columnName = columnList.get(out);
				for(int index = 0;index < intersect.size();index++) {
					String key = intersect.get(index); // {key}-{key}
					String testValues = testKeyIndexMap.get(key);// {value}-{value}
					String values = keyIndexMap.get(key);// {value}-{value}
					if(!testValues.equals(values)) { 
						String[] split = testValues.split(separator);
						LinkedList<String> dataList = updateMap.get(columnName);
						if(dataList == null) {
							dataList = new LinkedList<String>();
							updateMap.put(columnName,dataList);
						}
						dataList.add(split[out]);
					}
				}
				for(int index = 0;index < complement.size();index++) {
					String key = complement.get(index);
					if(testKeyIndexMap.containsKey(key)) { // insert
						String value = testKeyIndexMap.get(key);
						String[] split = value.split(separator);
						LinkedList<String> dataList = insertMap.get(columnName);
						if(dataList == null) {
							dataList = new LinkedList<String>();
							insertMap.put(columnName,dataList);
						}
						dataList.add(split[out]);
					}
					if(keyIndexMap.containsKey(key)) { // delete
						String value = keyIndexMap.get(key);
						String[] split = value.split(separator);
						LinkedList<String> dataList = deleteMap.get(columnName);
						if(dataList == null) {
							dataList = new LinkedList<String>();
							deleteMap.put(columnName,dataList);
						}
						dataList.add(split[out]);
					}
				}
			}
			if(updateMap.size() > 0) {
				ModifyInfos updateModifyInfos = ModifyInfoBuilder.createUpdateModifyInfos(tableName, linkedHashSet, updateMap);
				modifyInfos.add(updateModifyInfos);
			}
			if(insertMap.size() > 0) {
				ModifyInfos insertModifyInfos = ModifyInfoBuilder.createInsertModifyInfos(tableName, insertMap);
				modifyInfos.add(insertModifyInfos);
			}
			if(deleteMap.size() > 0) {
				ModifyInfos deleteModifyInfos = ModifyInfoBuilder.createDeleteModifyInfos(tableName, linkedHashSet, deleteMap);
				modifyInfos.add(deleteModifyInfos);
			}
		}
		logger.info("not compare table data tables:" + tableList);
		return modifyInfos;
	}
	
	private List<ModifyInfos> compareProcedure(DBInstanceNameSpace testDBInstanceNameSpace, DBInstanceNameSpace dbInstanceNameSpace){
		List<ModifyInfos> modifyInfos = new ArrayList<ModifyInfos>();
		Map<String, LinkedList<String>> testProcedures = testDBInstanceNameSpace.getProcedures();
		Map<String, LinkedList<String>> procedures = dbInstanceNameSpace.getProcedures();
		Set<String> keySet = testProcedures.keySet();
		Set<String> keySet_ = procedures.keySet();
		if(!Collections.compareCollection(keySet, keySet_)) {
			List<String> complement = Collections.complement(keySet, keySet_);
			for(int index = 0; index < complement.size();index ++) {
				String procedureName = complement.get(index);
				if(procedures.containsKey(procedureName)) { // drop
					ModifyInfos dropProcedureModifyInfos = ModifyInfoBuilder.createDropProcedureModifyInfos(procedureName);
					modifyInfos.add(dropProcedureModifyInfos);
				}
				if(testProcedures.containsKey(procedureName)) { // add
					ModifyInfos addProcedureModifyInfos = ModifyInfoBuilder.createAddProcedureModifyInfos(procedureName, testProcedures.get(procedureName), null);
					modifyInfos.add(addProcedureModifyInfos);
				}
			}
		}
		List<String> intersect = Collections.intersect(keySet, keySet_);
		for(int index = 0; index < intersect.size();index ++) {
			String procedureName = intersect.get(index);
			LinkedList<String> testLineList = testProcedures.get(procedureName);
			LinkedList<String> lineList = procedures.get(procedureName);
			boolean isSame = Collections.compareCollection(testLineList, lineList);
			Map<Integer,String> diffMap = new HashMap<Integer, String>();
			for(int in = 0;in < testLineList.size();in++) {
				String testLine = testLineList.get(in);
				String line = lineList.get(in);
				if(line != null) {
					if(!testLine.equals(line)) {
						diffMap.put(in,line.trim()+ " -> "+testLine.trim());
					}
				}else {
					diffMap.put(in," -> "+testLine.trim());
				}
			}
			if(!isSame) {
				ModifyInfos addProcedureModifyInfos = ModifyInfoBuilder.createAddProcedureModifyInfos(procedureName, testLineList, diffMap);
				modifyInfos.add(addProcedureModifyInfos);
			}
		}
		return modifyInfos;
	}
	
	private List<ModifyInfos> compareEvent(DBInstanceNameSpace testDBInstanceNameSpace, DBInstanceNameSpace dbInstanceNameSpace){
		List<ModifyInfos> modifyInfos = new ArrayList<ModifyInfos>();
		
		return modifyInfos;
	}
	

}
