package com.db.data.cache;

public class ModifyInfos {
	
	private String annotaion;
	
	private String modifyInfo;
	
	public ModifyInfos(String annotaion, String modifyInfo) {
		this.annotaion = annotaion;
		this.modifyInfo = modifyInfo;
	}

	public String getAnnotaion() {
		return annotaion;
	}

	public void setAnnotaion(String annotaion) {
		this.annotaion = annotaion;
	}

	public String getModifyInfo() {
		return modifyInfo;
	}

	public void setModifyInfo(String modifyInfo) {
		this.modifyInfo = modifyInfo;
	}

	@Override
	public String toString() {
		return "ModifyInfos [annotaion=" + annotaion + ", modifyInfo=" + modifyInfo + "]";
	}
	
}
