package com.kohaku.manager.model;

import lombok.Data;
import lombok.AllArgsConstructor;

@Data
@AllArgsConstructor
public class TaskStatus {
    private String id;
    private String type; // "DOWNLOAD" or "UPLOAD"
    private String status; // "RUNNING", "COMPLETED", "ERROR"
    private int progress; // 0-100
    private String currentFile;
    private String message;
    
    public TaskStatus(String id, String type) {
        this.setId(id);
        this.setType(type);
        this.setStatus("PENDING");
        this.setProgress(0);
        this.setCurrentFile("");
        this.setMessage("准备中...");
    }

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public int getProgress() {
		return progress;
	}

	public void setProgress(int progress) {
		this.progress = progress;
	}

	public String getCurrentFile() {
		return currentFile;
	}

	public void setCurrentFile(String currentFile) {
		this.currentFile = currentFile;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
}
