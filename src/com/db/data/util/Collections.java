package com.db.data.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Collections {
	/**
	 *	  对比
	 * @param first
	 * @param second
	 * @return
	 */
	public static boolean compareCollection(Collection<?> first,Collection<?> second) {
		if(first == null || second == null) {
			return Boolean.FALSE;
		}
		if(first.size() != second.size()) {
			return Boolean.FALSE;
		}else {
			List<Object> tmpList = new ArrayList<Object>();
			tmpList.addAll(first);
			tmpList.addAll(second);
			tmpList.removeAll(first);
			if(tmpList.size() > 0) { 
				return Boolean.FALSE;
			}
		}
		return Boolean.TRUE;
	}
	/**
	 * 	取交集
	 * @param <V>
	 * @param first
	 * @param second
	 * @return
	 */
	public static <V> List<V> intersect(Collection<V> first,Collection<V> second){
		if(first != null && second != null) {
			ArrayList<V> totalList = new ArrayList<>(first);
			totalList.removeAll(second);
			totalList.addAll(second);
			ArrayList<V> totalListTmp = new ArrayList<>(totalList);
			ArrayList<V> totalListTmp_ = new ArrayList<>(totalList);
			totalListTmp.removeAll(second);
			totalListTmp_.removeAll(first);
			totalList.removeAll(totalListTmp);
			totalList.removeAll(totalListTmp_);
			return totalList;
		}
		return null;
	}
	/**
	 * 	 合并
	 * @param <V>
	 * @param first
	 * @param second
	 * @return
	 */
	public static <V> List<V> merge(Collection<V> first,Collection<V> second){
		if(first != null && second != null) {
			first.addAll(second);
			return new ArrayList<>(first);
		}
		return null;
	}
	/**
	 * 	  去重
	 * @param <V>
	 * @param first
	 * @param second
	 * @return
	 */
	public static <V> List<V> distinct(Collection<V> first,Collection<V> second){
		if(first != null && second != null) {
			ArrayList<V> firstList = new ArrayList<>(first);
			ArrayList<V> secondList = new ArrayList<>(second);
			firstList.removeAll(secondList);
			firstList.addAll(secondList);
			return firstList;
		}
		return null;
	}
	/**
	 *  	取补集
	 * @param <V>
	 * @param first
	 * @param second
	 * @return
	 */
	public static <V> List<V> complement(Collection<V> first,Collection<V> second){
		if(first != null && second != null) {
			ArrayList<V> totalList = new ArrayList<>(first);
			totalList.addAll(second);
			ArrayList<V> totalListTmp = new ArrayList<>(totalList);
			ArrayList<V> totalListTmp_ = new ArrayList<>(totalList);
			totalListTmp.removeAll(second);
			totalListTmp_.removeAll(first);
			totalListTmp.addAll(totalListTmp_);
			return totalListTmp;
		}
		return null;
	}
}
