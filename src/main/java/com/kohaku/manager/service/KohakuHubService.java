package com.kohaku.manager.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kohaku.manager.model.TaskStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KohakuHubService {
	private static final Logger logger = LoggerFactory.getLogger(KohakuHubService.class);
	private final ObjectMapper mapper = new ObjectMapper();

	// 【添加】用于存储任务状态的线程安全 Map
	private final ConcurrentHashMap<String, TaskStatus> taskMap = new ConcurrentHashMap<String, TaskStatus>();

	public ConcurrentHashMap<String, TaskStatus> getAllTasks() {
		return taskMap;
	}
    /**
     * 检查仓库是否存在 (严格遵循文档: GET /api/models/{namespace}/{name})
     */
    private boolean checkRepository(String apiUrl, String token, String repoId) throws IOException {
        String[] parts = repoId.split("/");
        String namespace = parts.length > 1 ? parts[0] : "root"; // 假设单段为用户自己的，这里简化处理，通常单段需要根据 token 获取 username，或者作为默认 namespace
        String name = parts[parts.length - 1];

        // 构造 URL: /api/models/{namespace}/{name}
        String url = String.format("%s/api/models/%s/%s", 
            apiUrl, 
            URLEncoder.encode(namespace, StandardCharsets.UTF_8), 
            URLEncoder.encode(name, StandardCharsets.UTF_8)
        );

        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");
        if (token != null) {
            conn.setRequestProperty("Authorization", "Bearer " + token);
        }

        int responseCode = conn.getResponseCode();
        return (responseCode == 200);
    }

    /**
     * 创建新仓库 (严格遵循文档: POST /api/repos/create)
     */
    private void createRepository(String apiUrl, String token, String repoId) throws IOException {
        String url = apiUrl + "/api/repos/create";
        
        // 解析 repoId: "Qwen/ModelName" -> org="Qwen", name="ModelName"
        String[] parts = repoId.split("/");
        String org = null;
        String name = repoId;
        
        if (parts.length > 1) {
            org = parts[0];
            name = parts[1];
        }

        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Bearer " + token);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        // 构造符合文档的 JSON Body
        // organization 为 null 时，JSON 中必须写 null，不能写 "null"
        String orgJson = (org == null) ? "null" : "\"" + escape(org) + "\"";
        
        String jsonPayload = String.format(
            "{\"type\":\"model\",\"name\":\"%s\",\"organization\":%s,\"private\":false}",
            escape(name), orgJson
        );

        try (var os = conn.getOutputStream()) {
            os.write(jsonPayload.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = conn.getResponseCode();
        
        // 200 OK 表示创建成功
        // 400 Bad Request 可能表示仓库已存在，我们可以忽略或视为成功
        if (responseCode == 200 || responseCode == 201) {
            logger.info(">>> Repository {} created successfully.", repoId);
        } else if (responseCode == 400) {
            // 尝试读取错误信息，如果是因为已存在，则不算错误
            String err = "";
            if (conn.getErrorStream() != null) err = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            if (err.contains("already exists") || err.contains("conflict")) {
                logger.info(">>> Repository {} already exists (created concurrently?).", repoId);
            } else {
                throw new IOException("Failed to create repository " + repoId + ": " + responseCode + " - " + err);
            }
        } else {
            String err = "";
            if (conn.getErrorStream() != null) err = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            throw new IOException("Failed to create repository " + repoId + ": " + responseCode + " - " + err);
        }
    }

    /**
     * 检查仓库是否存在
     */
    private boolean checkRepository1(String apiUrl, String token, String repoId) throws IOException {
        String url = String.format("%s/models/%s", apiUrl, repoId);
        
        logger.info(">>> Repository url={}.", url);
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", "Bearer " + token);

        int responseCode = conn.getResponseCode();
        // 200 OK 表示存在，404 Not Found 表示不存在
        return (responseCode == 200);
    }

    /**
     * 创建新仓库
     */
    private void createRepository1(String apiUrl, String token, String repoId) throws IOException {
        String url = apiUrl + "/models";
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Bearer " + token);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        // 构造创建仓库的 JSON Payload
        // 注意：这里设置为 "public"，如果需要私有库可改为 "private"
        String jsonPayload = String.format(
            "{\"repo_id\":\"%s\",\"type\":\"model\",\"visibility\":\"public\"}", 
            repoId
        );

        try (var os = conn.getOutputStream()) {
            os.write(jsonPayload.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = conn.getResponseCode();
        if (responseCode != 200 && responseCode != 201) { // 201 Created 是标准，有些实现可能返回 200
            String err = "";
            if (conn.getErrorStream() != null) err = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            throw new IOException("Failed to create repository " + repoId + ": " + responseCode + " - " + err);
        }
        logger.info(">>> Repository {} created successfully.", repoId);
    }
    
	public void uploadModel(String taskId, String modelName, Path localDir, String repoId, String apiUrl, String token,
			long lfsThreshold, long multiThreshold, long chunkSize) throws IOException {

		logger.info(">>> [{}] Starting upload task: {}", taskId, repoId);

		TaskStatus task = new TaskStatus(taskId, modelName);
		taskMap.put(taskId, task);
		try {
// Step 1: Scan local files
			List<LocalFile> files = scanFiles(localDir);
			logger.info(">>> [{}] Found {} files to process.", taskId, files.size());

// --- 【智能路径修正】---
			if (!files.isEmpty()) {
				String firstPath = files.get(0).relativePath.replace('\\', '/');
				int firstSlashIdx = firstPath.indexOf('/');
				if (firstSlashIdx > 0) {
					String candidatePrefix = firstPath.substring(0, firstSlashIdx + 1);
					boolean allSameParent = true;
					for (LocalFile f : files) {
						String currentPath = f.relativePath.replace('\\', '/');
						if (!currentPath.startsWith(candidatePrefix)) {
							allSameParent = false;
							break;
						}
					}
					if (allSameParent) {
						logger.info(">>> Auto-stripping path prefix: {} (Files will be uploaded to repo root)",
								candidatePrefix);
						for (LocalFile f : files) {
							f.relativePath = f.relativePath.substring(candidatePrefix.length());
						}
					}
				}
			}
// -------------------------

// --- 【新增】检查并创建仓库 ---
			logger.info(">>> Repository {} Checking...", repoId);
			if (!checkRepository(apiUrl, token, repoId)) {
				logger.info(">>> Repository {} not found. Creating...", repoId);
				createRepository(apiUrl, token, repoId);
			}else {
				logger.info(">>> Repository {} found. go on...", repoId);
			}
// ---------------------------

// Step 2: Preupload (Check mode & duplicates)
			List<PreuploadResult> results = preupload(apiUrl, token, repoId, files);
			logger.info(">>> [{}] Preupload check complete.", taskId);

// Step 3: Process and Upload files
			List<String> commitLines = new ArrayList<>();
// Header line (Required)
			commitLines.add(
					"{\"key\":\"header\",\"value\":{\"summary\":\"Upload model\",\"description\":\"Uploaded via Kohaku Manager\"}}");

			int processedCount = 0;
			for (int i = 0; i < results.size(); i++) {
				PreuploadResult res = results.get(i);
				LocalFile file = res.localFile;

				logger.info(">>> [{}] Processing [{}/{}]: {}", taskId, (i + 1), files.size(), file.relativePath);

				if (res.shouldIgnore) {
					logger.info(">>> [{}] Skipping (already exists): {}", taskId, file.relativePath);
					continue;
				}

				try {
					if ("lfs".equalsIgnoreCase(res.uploadMode)) {
						// LFS Upload
						handleLfsUpload(apiUrl, repoId, token, file);
						commitLines.add(String.format(
								"{\"key\":\"lfsFile\",\"value\":{\"path\":\"%s\",\"oid\":\"%s\",\"size\":%d,\"algo\":\"sha256\"}}",
								escape(file.relativePath), file.sha256, file.size));
					} else {
						// Regular File (Base64 inline)
						String base64Content = Base64.getEncoder()
								.encodeToString(Files.readAllBytes(file.absolutePath));
						commitLines.add(String.format(
								"{\"key\":\"file\",\"value\":{\"path\":\"%s\",\"content\":\"%s\",\"encoding\":\"base64\"}}",
								escape(file.relativePath), base64Content));
					}
					processedCount++;
				} catch (Exception e) {
					logger.error(">>> [{}] Failed to process {}: {}", taskId, file.relativePath, e.getMessage(), e);
					throw new IOException("Upload failed for " + file.relativePath, e);
				}
			}

// Step 4: Commit
			if (processedCount > 0) {
				commitChanges(apiUrl, token, repoId, commitLines);
				logger.info(">>> [{}] Success. {} files committed.", taskId, processedCount);
			} else {
				logger.info(">>> [{}] No files to upload (all ignored).", taskId);
			}
			task.setStatus("COMPLETED");
			task.setMessage("Upload successful");
		} catch (Exception e) {
			task.setStatus("FAILED");
			task.setMessage(e.getMessage());
			logger.error(">>> [{}] Task failed.", taskId, e);
			throw e;
		}
	}

	public void uploadModel2(String taskId, String modelName, Path localDir, String repoId, String apiUrl, String token,
			long lfsThreshold, long multiThreshold, long chunkSize) throws IOException {

		logger.info(">>> [{}] Starting upload task: {}", taskId, repoId);

		TaskStatus task = new TaskStatus(taskId, modelName);
		taskMap.put(taskId, task);
		try {
// Step 1: Scan local files
			List<LocalFile> files = scanFiles(localDir);
			logger.info(">>> [{}] Found {} files to process.", taskId, files.size());

// --- 【修正版】智能路径修正 ---
			if (!files.isEmpty()) {
// 获取第一个文件的路径字符串，统一替换为 / 避免系统差异
				String firstPath = files.get(0).relativePath.replace('\\', '/');

// 查找第一个分隔符的位置，确定第一级文件夹
				int firstSlashIdx = firstPath.indexOf('/');

// 如果存在文件夹层级（即第一个 / 存在且不是在开头）
				if (firstSlashIdx > 0) {
					// 提取公共前缀，例如 "Qwen_Qwen2.5-0.5B-Instruct/"
					String candidatePrefix = firstPath.substring(0, firstSlashIdx + 1);

					boolean allSameParent = true;
					// 检查所有文件是否都以这个前缀开头
					for (LocalFile f : files) {
						String currentPath = f.relativePath.replace('\\', '/');
						if (!currentPath.startsWith(candidatePrefix)) {
							allSameParent = false;
							break;
						}
					}

					// 如果所有文件都在同一个子文件夹里，剥离这一层
					if (allSameParent) {
						logger.info(">>> Auto-stripping path prefix: {} (Files will be uploaded to repo root)",
								candidatePrefix);
						for (LocalFile f : files) {
							// 字符串截取：去掉前缀长度
							f.relativePath = f.relativePath.substring(candidatePrefix.length());
						}
					}
				}
			}
// --------------------------

// Step 2: Preupload (Check mode & duplicates)
			List<PreuploadResult> results = preupload(apiUrl, token, repoId, files);
			logger.info(">>> [{}] Preupload check complete.", taskId);

// Step 3: Process and Upload files
			List<String> commitLines = new ArrayList<>();
// Header line (Required)
			commitLines.add(
					"{\"key\":\"header\",\"value\":{\"summary\":\"Upload model\",\"description\":\"Uploaded via Kohaku Manager\"}}");

			int processedCount = 0;
			for (int i = 0; i < results.size(); i++) {
				PreuploadResult res = results.get(i);
				LocalFile file = res.localFile;

				logger.info(">>> [{}] Processing [{}/{}]: {}", taskId, (i + 1), files.size(), file.relativePath);

				if (res.shouldIgnore) {
					logger.info(">>> [{}] Skipping (already exists): {}", taskId, file.relativePath);
					continue;
				}

				try {
					if ("lfs".equalsIgnoreCase(res.uploadMode)) {
						// LFS Upload
						handleLfsUpload(apiUrl, repoId, token, file);
						commitLines.add(String.format(
								"{\"key\":\"lfsFile\",\"value\":{\"path\":\"%s\",\"oid\":\"%s\",\"size\":%d,\"algo\":\"sha256\"}}",
								escape(file.relativePath), file.sha256, file.size));
					} else {
						// Regular File (Base64 inline)
						String base64Content = Base64.getEncoder()
								.encodeToString(Files.readAllBytes(file.absolutePath));
						commitLines.add(String.format(
								"{\"key\":\"file\",\"value\":{\"path\":\"%s\",\"content\":\"%s\",\"encoding\":\"base64\"}}",
								escape(file.relativePath), base64Content));
					}
					processedCount++;
				} catch (Exception e) {
					logger.error(">>> [{}] Failed to process {}: {}", taskId, file.relativePath, e.getMessage(), e);
					throw new IOException("Upload failed for " + file.relativePath, e);
				}
			}

// Step 4: Commit
			if (processedCount > 0) {
				commitChanges(apiUrl, token, repoId, commitLines);
				logger.info(">>> [{}] Success. {} files committed.", taskId, processedCount);
			} else {
				logger.info(">>> [{}] No files to upload (all ignored).", taskId);
			}
			task.setStatus("COMPLETED");
			task.setMessage("Upload successful");
		} catch (Exception e) {
			task.setStatus("FAILED");
			task.setMessage(e.getMessage());
			logger.error(">>> [{}] Task failed.", taskId, e);
			throw e;
		}
	}

	public void uploadModel1(String taskId, String modelName, Path localDir, String repoId, String apiUrl, String token,
			long lfsThreshold, long multiThreshold, long chunkSize) throws IOException {

		logger.info(">>> [{}] Starting upload task: {}", taskId, repoId);

		// 【添加】初始化任务状态
		TaskStatus task = new TaskStatus(taskId, modelName);
		taskMap.put(taskId, task);
		try {
			// Step 1: Scan local files
			List<LocalFile> files = scanFiles(localDir);
			logger.info(">>> [{}] Found {} files to process.", taskId, files.size());

			// Step 2: Preupload (Check mode & duplicates)
			List<PreuploadResult> results = preupload(apiUrl, token, repoId, files);
			logger.info(">>> [{}] Preupload check complete.", taskId);

			// Step 3: Process and Upload files
			List<String> commitLines = new ArrayList<>();
			// Header line (Required)
			commitLines.add(
					"{\"key\":\"header\",\"value\":{\"summary\":\"Upload model\",\"description\":\"Uploaded via Kohaku Manager\"}}");

			int processedCount = 0;
			for (int i = 0; i < results.size(); i++) {
				PreuploadResult res = results.get(i);
				LocalFile file = res.localFile;

				logger.info(">>> [{}] Processing [{}/{}]: {}", taskId, (i + 1), files.size(), file.relativePath);
				// task.setCurrentFile(i + 1); // 【添加】更新当前进度

				if (res.shouldIgnore) {
					logger.info(">>> [{}] Skipping (already exists): {}", taskId, file.relativePath);
					continue;
				}

				try {
					if ("lfs".equalsIgnoreCase(res.uploadMode)) {
						// LFS Upload
						handleLfsUpload(apiUrl, repoId, token, file);
						commitLines.add(String.format(
								"{\"key\":\"lfsFile\",\"value\":{\"path\":\"%s\",\"oid\":\"%s\",\"size\":%d,\"algo\":\"sha256\"}}",
								escape(file.relativePath), file.sha256, file.size));
					} else {
						// Regular File (Base64 inline)
						String base64Content = Base64.getEncoder()
								.encodeToString(Files.readAllBytes(file.absolutePath));
						commitLines.add(String.format(
								"{\"key\":\"file\",\"value\":{\"path\":\"%s\",\"content\":\"%s\",\"encoding\":\"base64\"}}",
								escape(file.relativePath), base64Content));
					}
					processedCount++;
				} catch (Exception e) {
					logger.error(">>> [{}] Failed to process {}: {}", taskId, file.relativePath, e.getMessage(), e);
					throw new IOException("Upload failed for " + file.relativePath, e);
				}
			}

			// Step 4: Commit
			if (processedCount > 0) {
				commitChanges(apiUrl, token, repoId, commitLines);
				logger.info(">>> [{}] Success. {} files committed.", taskId, processedCount);
			} else {
				logger.info(">>> [{}] No files to upload (all ignored).", taskId);
			}
			// 【添加】任务成功
			task.setStatus("COMPLETED");
			task.setMessage("Upload successful");
		} catch (Exception e) {
			// 【添加】任务失败
			task.setStatus("FAILED");
			task.setMessage(e.getMessage());
			logger.error(">>> [{}] Task failed.", taskId, e);
			throw e;
		}
	}

	// --- Private Helper Methods ---

	private List<LocalFile> scanFiles(Path root) throws IOException {
		List<LocalFile> list = new ArrayList<>();
		try (var stream = Files.walk(root)) {
			stream.filter(Files::isRegularFile).forEach(path -> {
				try {
					LocalFile f = new LocalFile();
					f.relativePath = root.relativize(path).toString().replace("\\", "/");
					f.absolutePath = path;
					f.size = Files.size(path);
					f.sha256 = calculateSha256(path);
					list.add(f);
				} catch (Exception e) {
					logger.warn("Skipping file due to error: {}", path);
				}
			});
		}
		return list;
	}

	private String calculateSha256(Path path) throws Exception {
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		try (InputStream is = Files.newInputStream(path)) {
			byte[] buffer = new byte[8192];
			int read;
			while ((read = is.read(buffer)) > 0) {
				digest.update(buffer, 0, read);
			}
		}
		byte[] hashBytes = digest.digest();
		StringBuilder sb = new StringBuilder();
		for (byte b : hashBytes)
			sb.append(String.format("%02x", b));
		return sb.toString();
	}

	private List<PreuploadResult> preupload(String baseUrl, String token, String repoId, List<LocalFile> files)
			throws IOException {
		String url = String.format("%s/api/models/%s/preupload/main", baseUrl,
				URLEncoder.encode(repoId, StandardCharsets.UTF_8));
		HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Authorization", "Bearer " + token);
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setDoOutput(true);

		// Construct JSON body
		StringBuilder sb = new StringBuilder("{\"files\":[");
		for (int i = 0; i < files.size(); i++) {
			LocalFile f = files.get(i);
			sb.append(String.format("{\"path\":\"%s\",\"size\":%d,\"sha256\":\"%s\"}", escape(f.relativePath), f.size,
					f.sha256));
			if (i < files.size() - 1)
				sb.append(",");
		}
		sb.append("]}");

		try (var os = conn.getOutputStream()) {
			os.write(sb.toString().getBytes(StandardCharsets.UTF_8));
		}

		int code = conn.getResponseCode();
		if (code != 200) {
			String err = "";
			if (conn.getErrorStream() != null)
				err = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
			throw new IOException("Preupload failed " + code + ": " + err);
		}

		JsonNode root = mapper.readTree(conn.getInputStream());
		JsonNode filesNode = root.get("files");
		List<PreuploadResult> results = new ArrayList<>();

		if (filesNode != null && filesNode.isArray()) {
			for (int i = 0; i < filesNode.size(); i++) {
				JsonNode node = filesNode.get(i);
				PreuploadResult res = new PreuploadResult();
				res.localFile = files.get(i);
				res.uploadMode = node.has("uploadMode") ? node.get("uploadMode").asText() : "regular";
				res.shouldIgnore = node.has("shouldIgnore") && node.get("shouldIgnore").asBoolean();
				results.add(res);
			}
		}
		return results;
	}

	/**
	 * 严格按照 file-upload.md 实现 LFS 上传 包含 Single-Part 和 Multipart 的判断与执行
	 */
	private void handleLfsUpload(String baseUrl, String repoId, String token, LocalFile file) throws IOException {
		logger.info(">>> [LFS] Batch request for: {}", file.relativePath);

		// 1. Batch Request
		String batchUrl = String.format("%s/%s.git/info/lfs/objects/batch", baseUrl,
				URLEncoder.encode(repoId, StandardCharsets.UTF_8));
		HttpURLConnection conn = (HttpURLConnection) new URL(batchUrl).openConnection();
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Accept", "application/vnd.git-lfs+json");
		conn.setRequestProperty("Authorization", "Bearer " + token);
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setDoOutput(true);

		String batchBody = String.format(
				"{\"operation\":\"upload\",\"transfers\":[\"basic\"],\"objects\":[{\"oid\":\"%s\",\"size\":%d}]}",
				file.sha256, file.size);
		try (var os = conn.getOutputStream()) {
			os.write(batchBody.getBytes(StandardCharsets.UTF_8));
		}

		int code = conn.getResponseCode();
		if (code != 200) {
			String err = "";
			if (conn.getErrorStream() != null)
				err = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
			throw new IOException("LFS Batch failed " + code + ": " + err);
		}

		JsonNode root = mapper.readTree(conn.getInputStream());
		JsonNode objectNode = root.get("objects").get(0);

		// Check if actions exist (if not, file already exists)
		if (!objectNode.has("actions")) {
			logger.info(">>> [LFS] File already exists in storage (Deduplication). Skipping upload.");
			return;
		}

		JsonNode actions = objectNode.get("actions");
		JsonNode uploadNode = actions.get("upload");
		JsonNode headerNode = uploadNode.get("header"); // May be null

		// === 分流：Single-Part 或 Multipart ===
		if (headerNode != null && headerNode.has("chunk_size")) {
			// Multipart Upload
			logger.info(">>> [LFS] Mode: Multipart (Size: {})", file.size);
			executeMultipartUpload(headerNode, file, baseUrl, repoId, token);
		} else {
			// Single-Part Upload
			logger.info(">>> [LFS] Mode: Single-Part");
			executeSinglePartUpload(uploadNode, file);
		}

		// Verify (Common)
		if (actions.has("verify")) {
			verifyLfsObject(actions.get("verify").get("href").asText(), token, file);
		}
	}

	private void executeSinglePartUpload(JsonNode uploadNode, LocalFile file) throws IOException {
		String href = uploadNode.get("href").asText();

		HttpURLConnection upConn = (HttpURLConnection) new URL(href).openConnection();
		upConn.setRequestMethod("PUT");
		upConn.setFixedLengthStreamingMode(file.size);
		upConn.setDoOutput(true);

		// 设置服务端要求的 Header (如果有)
		JsonNode headers = uploadNode.get("header");
		if (headers != null) {
			Iterator<Map.Entry<String, JsonNode>> it = headers.fields();
			while (it.hasNext()) {
				Map.Entry<String, JsonNode> entry = it.next();
				upConn.setRequestProperty(entry.getKey(), entry.getValue().asText());
			}
		}

		try (var os = upConn.getOutputStream()) {
			Files.copy(file.absolutePath, os);
		}

		int upCode = upConn.getResponseCode();
		if (upCode != 200 && upCode != 201) {
			String err = "";
			if (upConn.getErrorStream() != null)
				err = new String(upConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
			throw new IOException("LFS Single-Part Upload failed " + upCode + ": " + err);
		}
		logger.info(">>> [LFS] Single-Part Upload Success.");
	}

	private void executeMultipartUpload(JsonNode headerNode, LocalFile file, String baseUrl, String repoId,
			String token) throws IOException {
		long chunkSize = headerNode.get("chunk_size").asLong();
		String uploadId = headerNode.get("upload_id").asText();
		int numParts = (int) Math.ceil((double) file.size / chunkSize);

		logger.info(">>> [LFS Multipart] Parts: {}, Chunk Size: {}, Upload ID: {}", numParts, chunkSize, uploadId);

		String targetHost = new URL(baseUrl).getHost();
		logger.info(">>> [LFS Multipart] Target Host for Parts: {}", targetHost);

		// --- 提取 Header ---
		String contentType = "application/octet-stream"; // 默认值
		Iterator<Map.Entry<String, JsonNode>> fields = headerNode.fields();
		while (fields.hasNext()) {
			Map.Entry<String, JsonNode> entry = fields.next();
			String key = entry.getKey();

			if (key.matches("\\d+") || "chunk_size".equals(key) || "upload_id".equals(key)) {
				continue;
			}

			if ("Content-Type".equalsIgnoreCase(key)) {
				contentType = entry.getValue().asText();
				logger.debug(">>> [LFS Multipart] Server specifies Content-Type: {}", contentType);
			} else {
				logger.warn(">>> [LFS Multipart] Ignoring Header: {}", key);
			}
		}

		List<PartInfo> uploadedParts = new ArrayList<>();

		try (InputStream is = Files.newInputStream(file.absolutePath)) {
			for (int partNumber = 1; partNumber <= numParts; partNumber++) {
				String partKey = String.valueOf(partNumber);
				if (!headerNode.has(partKey)) {
					throw new IOException("Missing URL for Part " + partNumber + " in header");
				}

				String partUrl = headerNode.get(partKey).asText();

				// 修正 Host IP (如果需要)
				if (partUrl.contains("127.0.0.1") || partUrl.contains("localhost")) {
					partUrl = partUrl.replace("://127.0.0.1", "://" + targetHost);
					partUrl = partUrl.replace("://localhost", "://" + targetHost);
				}

				logger.info(">>> [LFS Multipart] Uploading Part {} to: {}", partNumber, partUrl);

				// 读取分片数据
				long bytesToRead = Math.min(chunkSize, file.size - ((partNumber - 1) * chunkSize));
				byte[] buffer = new byte[(int) bytesToRead];
				int read = is.read(buffer);
				if (read == -1)
					break;
				if (read < bytesToRead)
					buffer = Arrays.copyOf(buffer, read);

				// 上传分片
				HttpURLConnection partConn = (HttpURLConnection) new URL(partUrl).openConnection();
				partConn.setRequestMethod("PUT");
				partConn.setFixedLengthStreamingMode(buffer.length);
				partConn.setDoOutput(true);

				// 禁用 Expect: 100-continue
				partConn.setRequestProperty("Expect", "");
				partConn.setRequestProperty("Content-Type", contentType);

				try (var os = partConn.getOutputStream()) {
					os.write(buffer);
				}

				int pCode = partConn.getResponseCode();
				if (pCode != 200) {
					String err = "";
					if (partConn.getErrorStream() != null)
						err = new String(partConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
					throw new IOException("LFS Part " + partNumber + " failed " + pCode + ": " + err);
				}

				String etag = partConn.getHeaderField("ETag");
				if (etag != null) {
					etag = etag.replace("\"", "");
				} else {
					throw new IOException("ETag missing for Part " + partNumber);
				}

				uploadedParts.add(new PartInfo(partNumber, etag));
				logger.info(">>> [LFS Multipart] Part {}/{} uploaded (ETag: {})", partNumber, numParts, etag);
			}
		}

		// --- 【关键修正】修改 Complete 接口 URL，添加 /api 路径 ---
		// 修改前: %s/%s.git/info/lfs/complete/%s
		// 修改后: %s/api/%s.git/info/lfs/complete/%s
		String completeUrl = String.format("%s/api/%s.git/info/lfs/complete/%s", baseUrl,
				URLEncoder.encode(repoId, StandardCharsets.UTF_8), URLEncoder.encode(uploadId, StandardCharsets.UTF_8) // 对
																														// uploadId
																														// 也进行编码
		);

		logger.info(">>> [LFS Multipart] Completing upload at: {}", completeUrl);

		HttpURLConnection completeConn = (HttpURLConnection) new URL(completeUrl).openConnection();
		completeConn.setRequestMethod("POST");
		completeConn.setRequestProperty("Content-Type", "application/json");
		completeConn.setRequestProperty("Authorization", "Bearer " + token);
		completeConn.setDoOutput(true);

		StringBuilder partsJson = new StringBuilder("{\"oid\":\"").append(file.sha256).append("\",\"size\":")
				.append(file.size).append(",\"upload_id\":\"").append(uploadId).append("\",\"parts\":[");

		for (int i = 0; i < uploadedParts.size(); i++) {
			PartInfo p = uploadedParts.get(i);
			partsJson.append("{\"PartNumber\":").append(p.partNumber).append(",\"ETag\":\"").append(p.etag)
					.append("\"}");
			if (i < uploadedParts.size() - 1)
				partsJson.append(",");
		}
		partsJson.append("]}");

		try (var os = completeConn.getOutputStream()) {
			os.write(partsJson.toString().getBytes(StandardCharsets.UTF_8));
		}

		int cCode = completeConn.getResponseCode();
		if (cCode != 200) {
			String err = "";
			if (completeConn.getErrorStream() != null)
				err = new String(completeConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
			throw new IOException("LFS Complete failed " + cCode + ": " + err);
		}
		logger.info(">>> [LFS Multipart] Upload Complete Success.");
	}

	private void executeMultipartUpload6(JsonNode headerNode, LocalFile file, String baseUrl, String repoId,
			String token) throws IOException {
		long chunkSize = headerNode.get("chunk_size").asLong();
		String uploadId = headerNode.get("upload_id").asText();
		int numParts = (int) Math.ceil((double) file.size / chunkSize);

		logger.info(">>> [LFS Multipart] Parts: {}, Chunk Size: {}, Upload ID: {}", numParts, chunkSize, uploadId);

		String targetHost = new URL(baseUrl).getHost();
		logger.info(">>> [LFS Multipart] Target Host for Parts: {}", targetHost);

		// --- 提取 Header ---
		String contentType = "application/octet-stream"; // 默认值
		Iterator<Map.Entry<String, JsonNode>> fields = headerNode.fields();
		while (fields.hasNext()) {
			Map.Entry<String, JsonNode> entry = fields.next();
			String key = entry.getKey();

			// 排除分片索引和配置键
			if (key.matches("\\d+") || "chunk_size".equals(key) || "upload_id".equals(key)) {
				continue;
			}

			// 提取 Content-Type
			if ("Content-Type".equalsIgnoreCase(key)) {
				contentType = entry.getValue().asText();
				logger.debug(">>> [LFS Multipart] Server specifies Content-Type: {}", contentType);
			} else {
				logger.warn(">>> [LFS Multipart] Ignoring Header: {}", key);
			}
		}

		List<PartInfo> uploadedParts = new ArrayList<>();

		try (InputStream is = Files.newInputStream(file.absolutePath)) {
			for (int partNumber = 1; partNumber <= numParts; partNumber++) {
				String partKey = String.valueOf(partNumber);
				if (!headerNode.has(partKey)) {
					throw new IOException("Missing URL for Part " + partNumber + " in header");
				}

				String partUrl = headerNode.get(partKey).asText();

				// 修正 Host IP (如果需要)
				if (partUrl.contains("127.0.0.1") || partUrl.contains("localhost")) {
					partUrl = partUrl.replace("://127.0.0.1", "://" + targetHost);
					partUrl = partUrl.replace("://localhost", "://" + targetHost);
				}

				logger.info(">>> [LFS Multipart] Uploading Part {} to: {}", partNumber, partUrl);

				// 读取分片数据
				long bytesToRead = Math.min(chunkSize, file.size - ((partNumber - 1) * chunkSize));
				byte[] buffer = new byte[(int) bytesToRead];
				int read = is.read(buffer);
				if (read == -1)
					break;
				if (read < bytesToRead)
					buffer = Arrays.copyOf(buffer, read);

				// 上传分片
				HttpURLConnection partConn = (HttpURLConnection) new URL(partUrl).openConnection();
				partConn.setRequestMethod("PUT");
				partConn.setFixedLengthStreamingMode(buffer.length);
				partConn.setDoOutput(true);

				// --- 【关键修正】显式禁用 Expect: 100-continue ---
				// 这强制 Java 客户端直接发送数据，避免握手失败导致的 Connection reset
				partConn.setRequestProperty("Expect", "");
				// -----------------------------------------

				// 设置 Content-Type
				partConn.setRequestProperty("Content-Type", contentType);

				try (var os = partConn.getOutputStream()) {
					os.write(buffer);
				}

				int pCode = partConn.getResponseCode();
				if (pCode != 200) {
					String err = "";
					if (partConn.getErrorStream() != null)
						err = new String(partConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
					throw new IOException("LFS Part " + partNumber + " failed " + pCode + ": " + err);
				}

				String etag = partConn.getHeaderField("ETag");
				if (etag != null) {
					etag = etag.replace("\"", "");
				} else {
					throw new IOException("ETag missing for Part " + partNumber);
				}

				uploadedParts.add(new PartInfo(partNumber, etag));
				logger.info(">>> [LFS Multipart] Part {}/{} uploaded (ETag: {})", partNumber, numParts, etag);
			}
		}

		// 完成分片上传
		String completeUrl = String.format("%s/%s.git/info/lfs/complete/%s", baseUrl,
				URLEncoder.encode(repoId, StandardCharsets.UTF_8), uploadId);

		logger.info(">>> [LFS Multipart] Completing upload at: {}", completeUrl);

		HttpURLConnection completeConn = (HttpURLConnection) new URL(completeUrl).openConnection();
		completeConn.setRequestMethod("POST");
		completeConn.setRequestProperty("Content-Type", "application/json");
		completeConn.setRequestProperty("Authorization", "Bearer " + token);
		completeConn.setDoOutput(true);

		StringBuilder partsJson = new StringBuilder("{\"oid\":\"").append(file.sha256).append("\",\"size\":")
				.append(file.size).append(",\"upload_id\":\"").append(uploadId).append("\",\"parts\":[");

		for (int i = 0; i < uploadedParts.size(); i++) {
			PartInfo p = uploadedParts.get(i);
			partsJson.append("{\"PartNumber\":").append(p.partNumber).append(",\"ETag\":\"").append(p.etag)
					.append("\"}");
			if (i < uploadedParts.size() - 1)
				partsJson.append(",");
		}
		partsJson.append("]}");

		try (var os = completeConn.getOutputStream()) {
			os.write(partsJson.toString().getBytes(StandardCharsets.UTF_8));
		}

		int cCode = completeConn.getResponseCode();
		if (cCode != 200) {
			String err = "";
			if (completeConn.getErrorStream() != null)
				err = new String(completeConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
			throw new IOException("LFS Complete failed " + cCode + ": " + err);
		}
		logger.info(">>> [LFS Multipart] Upload Complete Success.");
	}

	private void executeMultipartUpload5(JsonNode headerNode, LocalFile file, String baseUrl, String repoId,
			String token) throws IOException {
		long chunkSize = headerNode.get("chunk_size").asLong();
		String uploadId = headerNode.get("upload_id").asText();
		int numParts = (int) Math.ceil((double) file.size / chunkSize);

		logger.info(">>> [LFS Multipart] Parts: {}, Chunk Size: {}, Upload ID: {}", numParts, chunkSize, uploadId);

		String targetHost = new URL(baseUrl).getHost();
		logger.info(">>> [LFS Multipart] Target Host for Parts: {}", targetHost);

		// --- 【关键修正】安全提取 Header ---
		String contentType = null;
		Iterator<Map.Entry<String, JsonNode>> fields = headerNode.fields();
		while (fields.hasNext()) {
			Map.Entry<String, JsonNode> entry = fields.next();
			String key = entry.getKey();

			// 排除分片索引和配置键
			if (key.matches("\\d+") || "chunk_size".equals(key) || "upload_id".equals(key)) {
				continue;
			}

			// --- 只保留 Content-Type，忽略其他所有 Header ---
			if ("Content-Type".equalsIgnoreCase(key)) {
				contentType = entry.getValue().asText();
				logger.debug(">>> [LFS Multipart] Using Content-Type: {}", contentType);
			} else {
				logger.warn(">>> [LFS Multipart] Ignoring Header: {} = {} (Only Content-Type is allowed)", key,
						entry.getValue().asText());
			}
			// ----------------------------------------
		}

		List<PartInfo> uploadedParts = new ArrayList<>();

		try (InputStream is = Files.newInputStream(file.absolutePath)) {
			for (int partNumber = 1; partNumber <= numParts; partNumber++) {
				String partKey = String.valueOf(partNumber);
				if (!headerNode.has(partKey)) {
					throw new IOException("Missing URL for Part " + partNumber + " in header");
				}

				String partUrl = headerNode.get(partKey).asText();

				// 修正 Host IP (如果需要)
				if (partUrl.contains("127.0.0.1") || partUrl.contains("localhost")) {
					partUrl = partUrl.replace("://127.0.0.1", "://" + targetHost);
					partUrl = partUrl.replace("://localhost", "://" + targetHost);
				}

				logger.debug(">>> [LFS Multipart] Uploading Part {} to: {}", partNumber, partUrl);

				// 读取分片数据
				long bytesToRead = Math.min(chunkSize, file.size - ((partNumber - 1) * chunkSize));
				byte[] buffer = new byte[(int) bytesToRead];
				int read = is.read(buffer);
				if (read == -1)
					break;
				if (read < bytesToRead)
					buffer = Arrays.copyOf(buffer, read);

				// 上传分片
				HttpURLConnection partConn = (HttpURLConnection) new URL(partUrl).openConnection();
				partConn.setRequestMethod("PUT");
				partConn.setFixedLengthStreamingMode(buffer.length);
				partConn.setDoOutput(true);

				// --- 【关键修正】只设置 Content-Type (如果存在) ---
				if (contentType != null) {
					partConn.setRequestProperty("Content-Type", contentType);
				}
				// -------------------------------------------------

				try (var os = partConn.getOutputStream()) {
					os.write(buffer);
				}

				int pCode = partConn.getResponseCode();
				if (pCode != 200) {
					String err = "";
					if (partConn.getErrorStream() != null)
						err = new String(partConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
					throw new IOException("LFS Part " + partNumber + " failed " + pCode + ": " + err);
				}

				String etag = partConn.getHeaderField("ETag");
				if (etag != null) {
					etag = etag.replace("\"", "");
				} else {
					throw new IOException("ETag missing for Part " + partNumber);
				}

				uploadedParts.add(new PartInfo(partNumber, etag));
				logger.info(">>> [LFS Multipart] Part {}/{} uploaded (ETag: {})", partNumber, numParts, etag);
			}
		}

		// 完成分片上传
		String completeUrl = String.format("%s/%s.git/info/lfs/complete/%s", baseUrl,
				URLEncoder.encode(repoId, StandardCharsets.UTF_8), uploadId);

		logger.info(">>> [LFS Multipart] Completing upload at: {}", completeUrl);

		HttpURLConnection completeConn = (HttpURLConnection) new URL(completeUrl).openConnection();
		completeConn.setRequestMethod("POST");
		completeConn.setRequestProperty("Content-Type", "application/json");
		completeConn.setRequestProperty("Authorization", "Bearer " + token);
		completeConn.setDoOutput(true);

		StringBuilder partsJson = new StringBuilder("{\"oid\":\"").append(file.sha256).append("\",\"size\":")
				.append(file.size).append(",\"upload_id\":\"").append(uploadId).append("\",\"parts\":[");

		for (int i = 0; i < uploadedParts.size(); i++) {
			PartInfo p = uploadedParts.get(i);
			partsJson.append("{\"PartNumber\":").append(p.partNumber).append(",\"ETag\":\"").append(p.etag)
					.append("\"}");
			if (i < uploadedParts.size() - 1)
				partsJson.append(",");
		}
		partsJson.append("]}");

		try (var os = completeConn.getOutputStream()) {
			os.write(partsJson.toString().getBytes(StandardCharsets.UTF_8));
		}

		int cCode = completeConn.getResponseCode();
		if (cCode != 200) {
			String err = "";
			if (completeConn.getErrorStream() != null)
				err = new String(completeConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
			throw new IOException("LFS Complete failed " + cCode + ": " + err);
		}
		logger.info(">>> [LFS Multipart] Upload Complete Success.");
	}

	private void executeMultipartUpload4(JsonNode headerNode, LocalFile file, String baseUrl, String repoId,
			String token) throws IOException {
		long chunkSize = headerNode.get("chunk_size").asLong();
		String uploadId = headerNode.get("upload_id").asText();
		int numParts = (int) Math.ceil((double) file.size / chunkSize);

		logger.info(">>> [LFS Multipart] Parts: {}, Chunk Size: {}, Upload ID: {}", numParts, chunkSize, uploadId);

		String targetHost = new URL(baseUrl).getHost();
		logger.info(">>> [LFS Multipart] Target Host for Parts: {}", targetHost);

		// --- 【关键修正】提取所有非分片键作为请求头 ---
		Map<String, String> globalHeaders = new HashMap<>();
		Iterator<Map.Entry<String, JsonNode>> fields = headerNode.fields();
		while (fields.hasNext()) {
			Map.Entry<String, JsonNode> entry = fields.next();
			String key = entry.getKey();
			// 排除分片索引、chunk_size 和 upload_id
			if (!key.matches("\\d+") && !"chunk_size".equals(key) && !"upload_id".equals(key)) {
				globalHeaders.put(key, entry.getValue().asText());
				logger.debug(">>> [LFS Multipart] Found Header: {} = {}", key, entry.getValue().asText());
			}
		}
		// ------------------------------------------

		List<PartInfo> uploadedParts = new ArrayList<>();

		try (InputStream is = Files.newInputStream(file.absolutePath)) {
			for (int partNumber = 1; partNumber <= numParts; partNumber++) {
				String partKey = String.valueOf(partNumber);
				if (!headerNode.has(partKey)) {
					throw new IOException("Missing URL for Part " + partNumber + " in header");
				}

				String partUrl = headerNode.get(partKey).asText();

				// 修正 Host IP (如果需要)
				if (partUrl.contains("127.0.0.1") || partUrl.contains("localhost")) {
					partUrl = partUrl.replace("://127.0.0.1", "://" + targetHost);
					partUrl = partUrl.replace("://localhost", "://" + targetHost);
				}

				// 读取分片数据
				long bytesToRead = Math.min(chunkSize, file.size - ((partNumber - 1) * chunkSize));
				byte[] buffer = new byte[(int) bytesToRead];
				int read = is.read(buffer);
				if (read == -1)
					break;
				if (read < bytesToRead)
					buffer = Arrays.copyOf(buffer, read);

				// 上传分片
				HttpURLConnection partConn = (HttpURLConnection) new URL(partUrl).openConnection();
				partConn.setRequestMethod("PUT");
				partConn.setFixedLengthStreamingMode(buffer.length);
				partConn.setDoOutput(true);

				// --- 【关键修正】设置从 JSON 中提取的 Header，但排除 "Host" ---
				for (Map.Entry<String, String> header : globalHeaders.entrySet()) {
					// ！！！重点：不要手动设置 Host 头，让 Java 根据 URL 自动处理 ！！！
					if (!"Host".equalsIgnoreCase(header.getKey())) {
						partConn.setRequestProperty(header.getKey(), header.getValue());
					}
				}
				// ---------------------------------------------------

				try (var os = partConn.getOutputStream()) {
					os.write(buffer);
				}

				int pCode = partConn.getResponseCode();
				if (pCode != 200) {
					String err = "";
					if (partConn.getErrorStream() != null)
						err = new String(partConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
					throw new IOException("LFS Part " + partNumber + " failed " + pCode + ": " + err);
				}

				String etag = partConn.getHeaderField("ETag");
				if (etag != null) {
					etag = etag.replace("\"", "");
				} else {
					throw new IOException("ETag missing for Part " + partNumber);
				}

				uploadedParts.add(new PartInfo(partNumber, etag));
				logger.info(">>> [LFS Multipart] Part {}/{} uploaded (ETag: {})", partNumber, numParts, etag);
			}
		}

		// 完成分片上传
		String completeUrl = String.format("%s/%s.git/info/lfs/complete/%s", baseUrl,
				URLEncoder.encode(repoId, StandardCharsets.UTF_8), uploadId);

		logger.info(">>> [LFS Multipart] Completing upload at: {}", completeUrl);

		HttpURLConnection completeConn = (HttpURLConnection) new URL(completeUrl).openConnection();
		completeConn.setRequestMethod("POST");
		completeConn.setRequestProperty("Content-Type", "application/json");
		completeConn.setRequestProperty("Authorization", "Bearer " + token);
		completeConn.setDoOutput(true);

		StringBuilder partsJson = new StringBuilder("{\"oid\":\"").append(file.sha256).append("\",\"size\":")
				.append(file.size).append(",\"upload_id\":\"").append(uploadId).append("\",\"parts\":[");

		for (int i = 0; i < uploadedParts.size(); i++) {
			PartInfo p = uploadedParts.get(i);
			partsJson.append("{\"PartNumber\":").append(p.partNumber).append(",\"ETag\":\"").append(p.etag)
					.append("\"}");
			if (i < uploadedParts.size() - 1)
				partsJson.append(",");
		}
		partsJson.append("]}");

		try (var os = completeConn.getOutputStream()) {
			os.write(partsJson.toString().getBytes(StandardCharsets.UTF_8));
		}

		int cCode = completeConn.getResponseCode();
		if (cCode != 200) {
			String err = "";
			if (completeConn.getErrorStream() != null)
				err = new String(completeConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
			throw new IOException("LFS Complete failed " + cCode + ": " + err);
		}
		logger.info(">>> [LFS Multipart] Upload Complete Success.");
	}

	private void executeMultipartUpload3(JsonNode headerNode, LocalFile file, String baseUrl, String repoId,
			String token) throws IOException {
		long chunkSize = headerNode.get("chunk_size").asLong();
		String uploadId = headerNode.get("upload_id").asText();
		int numParts = (int) Math.ceil((double) file.size / chunkSize);

		logger.info(">>> [LFS Multipart] Parts: {}, Chunk Size: {}, Upload ID: {}", numParts, chunkSize, uploadId);

		String targetHost = new URL(baseUrl).getHost();
		logger.info(">>> [LFS Multipart] Target Host for Parts: {}", targetHost);

		// --- 【关键修正】提取所有非分片键作为请求头 ---
		Map<String, String> globalHeaders = new HashMap<>();
		Iterator<Map.Entry<String, JsonNode>> fields = headerNode.fields();
		while (fields.hasNext()) {
			Map.Entry<String, JsonNode> entry = fields.next();
			String key = entry.getKey();
			// 排除分片索引、chunk_size 和 upload_id
			if (!key.matches("\\d+") && !"chunk_size".equals(key) && !"upload_id".equals(key)) {
				globalHeaders.put(key, entry.getValue().asText());
				logger.debug(">>> [LFS Multipart] Found Header: {} = {}", key, entry.getValue().asText());
			}
		}
		// ------------------------------------------

		List<PartInfo> uploadedParts = new ArrayList<>();

		try (InputStream is = Files.newInputStream(file.absolutePath)) {
			for (int partNumber = 1; partNumber <= numParts; partNumber++) {
				String partKey = String.valueOf(partNumber);
				if (!headerNode.has(partKey)) {
					throw new IOException("Missing URL for Part " + partNumber + " in header");
				}

				String partUrl = headerNode.get(partKey).asText();

				// 修正 Host IP (如果需要)
				if (partUrl.contains("127.0.0.1") || partUrl.contains("localhost")) {
					partUrl = partUrl.replace("://127.0.0.1", "://" + targetHost);
					partUrl = partUrl.replace("://localhost", "://" + targetHost);
				}

				// 读取分片数据
				long bytesToRead = Math.min(chunkSize, file.size - ((partNumber - 1) * chunkSize));
				byte[] buffer = new byte[(int) bytesToRead];
				int read = is.read(buffer);
				if (read == -1)
					break;
				if (read < bytesToRead)
					buffer = Arrays.copyOf(buffer, read);

				// 上传分片
				HttpURLConnection partConn = (HttpURLConnection) new URL(partUrl).openConnection();
				partConn.setRequestMethod("PUT");
				partConn.setFixedLengthStreamingMode(buffer.length);
				partConn.setDoOutput(true);

				// --- 【关键修正】设置从 JSON 中提取的 Header ---
				for (Map.Entry<String, String> header : globalHeaders.entrySet()) {
					partConn.setRequestProperty(header.getKey(), header.getValue());
				}
				// ------------------------------------------

				try (var os = partConn.getOutputStream()) {
					os.write(buffer);
				}

				int pCode = partConn.getResponseCode();
				if (pCode != 200) {
					String err = "";
					if (partConn.getErrorStream() != null)
						err = new String(partConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
					throw new IOException("LFS Part " + partNumber + " failed " + pCode + ": " + err);
				}

				String etag = partConn.getHeaderField("ETag");
				if (etag != null) {
					etag = etag.replace("\"", "");
				} else {
					throw new IOException("ETag missing for Part " + partNumber);
				}

				uploadedParts.add(new PartInfo(partNumber, etag));
				logger.info(">>> [LFS Multipart] Part {}/{} uploaded (ETag: {})", partNumber, numParts, etag);
			}
		}

		// 完成分片上传
		String completeUrl = String.format("%s/%s.git/info/lfs/complete/%s", baseUrl,
				URLEncoder.encode(repoId, StandardCharsets.UTF_8), uploadId);

		logger.info(">>> [LFS Multipart] Completing upload at: {}", completeUrl);

		HttpURLConnection completeConn = (HttpURLConnection) new URL(completeUrl).openConnection();
		completeConn.setRequestMethod("POST");
		completeConn.setRequestProperty("Content-Type", "application/json");
		completeConn.setRequestProperty("Authorization", "Bearer " + token);
		completeConn.setDoOutput(true);

		StringBuilder partsJson = new StringBuilder("{\"oid\":\"").append(file.sha256).append("\",\"size\":")
				.append(file.size).append(",\"upload_id\":\"").append(uploadId).append("\",\"parts\":[");

		for (int i = 0; i < uploadedParts.size(); i++) {
			PartInfo p = uploadedParts.get(i);
			partsJson.append("{\"PartNumber\":").append(p.partNumber).append(",\"ETag\":\"").append(p.etag)
					.append("\"}");
			if (i < uploadedParts.size() - 1)
				partsJson.append(",");
		}
		partsJson.append("]}");

		try (var os = completeConn.getOutputStream()) {
			os.write(partsJson.toString().getBytes(StandardCharsets.UTF_8));
		}

		int cCode = completeConn.getResponseCode();
		if (cCode != 200) {
			String err = "";
			if (completeConn.getErrorStream() != null)
				err = new String(completeConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
			throw new IOException("LFS Complete failed " + cCode + ": " + err);
		}
		logger.info(">>> [LFS Multipart] Upload Complete Success.");
	}

	// 【修正】方法签名增加 String token
	private void executeMultipartUpload2(JsonNode headerNode, LocalFile file, String baseUrl, String repoId,
			String token) throws IOException {
		long chunkSize = headerNode.get("chunk_size").asLong();
		String uploadId = headerNode.get("upload_id").asText();
		int numParts = (int) Math.ceil((double) file.size / chunkSize);

		logger.info(">>> [LFS Multipart] Parts: {}, Chunk Size: {}, Upload ID: {}", numParts, chunkSize, uploadId);

		// --- 【关键修正】：提取正确的 Host，用于修正 Part URL ---
		String targetHost = new URL(baseUrl).getHost();
		logger.info(">>> [LFS Multipart] Target Host for Parts: {}", targetHost);
		// --------------------------------------------------

		List<PartInfo> uploadedParts = new ArrayList<>();

		try (InputStream is = Files.newInputStream(file.absolutePath)) {
			for (int partNumber = 1; partNumber <= numParts; partNumber++) {
				String partKey = String.valueOf(partNumber);
				if (!headerNode.has(partKey)) {
					throw new IOException("Missing URL for Part " + partNumber + " in header");
				}

				String partUrl = headerNode.get(partKey).asText();

				// --- 【关键修正】：替换分片 URL 中的 IP ---
				if (partUrl.contains("127.0.0.1") || partUrl.contains("localhost")) {
					partUrl = partUrl.replace("://127.0.0.1", "://" + targetHost);
					partUrl = partUrl.replace("://localhost", "://" + targetHost);
					// logger.debug(">>> [Part URL Corrected]: {}", partUrl);
				}
				// ------------------------------------------

				// 读取分片数据
				long bytesToRead = Math.min(chunkSize, file.size - ((partNumber - 1) * chunkSize));
				byte[] buffer = new byte[(int) bytesToRead];
				int read = is.read(buffer);
				if (read == -1)
					break;
				// 如果读取不满（文件结束），截断 buffer
				if (read < bytesToRead)
					buffer = Arrays.copyOf(buffer, read);

				// 上传分片
				HttpURLConnection partConn = (HttpURLConnection) new URL(partUrl).openConnection();
				partConn.setRequestMethod("PUT");
				partConn.setFixedLengthStreamingMode(buffer.length);
				partConn.setDoOutput(true);

				try (var os = partConn.getOutputStream()) {
					os.write(buffer);
				}

				int pCode = partConn.getResponseCode();
				if (pCode != 200) {
					String err = "";
					if (partConn.getErrorStream() != null)
						err = new String(partConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
					throw new IOException("LFS Part " + partNumber + " failed " + pCode + ": " + err);
				}

				// 获取 ETag
				String etag = partConn.getHeaderField("ETag");
				if (etag != null) {
					etag = etag.replace("\"", ""); // 去除引号
				} else {
					throw new IOException("ETag missing for Part " + partNumber);
				}

				uploadedParts.add(new PartInfo(partNumber, etag));
				logger.info(">>> [LFS Multipart] Part {}/{} uploaded (ETag: {})", partNumber, numParts, etag);
			}
		}

		// 完成分片上传
		// 文档 URL: POST /{repo}.git/info/lfs/complete/{upload_id}
		String completeUrl = String.format("%s/%s.git/info/lfs/complete/%s", baseUrl,
				URLEncoder.encode(repoId, StandardCharsets.UTF_8), uploadId);

		logger.info(">>> [LFS Multipart] Completing upload at: {}", completeUrl);

		HttpURLConnection completeConn = (HttpURLConnection) new URL(completeUrl).openConnection();
		completeConn.setRequestMethod("POST");
		completeConn.setRequestProperty("Content-Type", "application/json");
		completeConn.setRequestProperty("Authorization", "Bearer " + token);
		completeConn.setDoOutput(true);

		// 构建 Parts JSON
		StringBuilder partsJson = new StringBuilder("{\"oid\":\"").append(file.sha256).append("\",\"size\":")
				.append(file.size).append(",\"upload_id\":\"").append(uploadId).append("\",\"parts\":[");

		for (int i = 0; i < uploadedParts.size(); i++) {
			PartInfo p = uploadedParts.get(i);
			partsJson.append("{\"PartNumber\":").append(p.partNumber).append(",\"ETag\":\"").append(p.etag)
					.append("\"}");
			if (i < uploadedParts.size() - 1)
				partsJson.append(",");
		}
		partsJson.append("]}");

		try (var os = completeConn.getOutputStream()) {
			os.write(partsJson.toString().getBytes(StandardCharsets.UTF_8));
		}

		int cCode = completeConn.getResponseCode();
		if (cCode != 200) {
			String err = "";
			if (completeConn.getErrorStream() != null)
				err = new String(completeConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
			throw new IOException("LFS Complete failed " + cCode + ": " + err);
		}
		logger.info(">>> [LFS Multipart] Upload Complete Success.");
	}

	private void executeMultipartUpload1(JsonNode headerNode, LocalFile file, String baseUrl, String repoId,
			String token) throws IOException {
		long chunkSize = headerNode.get("chunk_size").asLong();
		String uploadId = headerNode.get("upload_id").asText();
		int numParts = (int) Math.ceil((double) file.size / chunkSize);

		logger.info(">>> [LFS Multipart] Parts: {}, Chunk Size: {}, Upload ID: {}", numParts, chunkSize, uploadId);

		List<PartInfo> uploadedParts = new ArrayList<>();

		try (InputStream is = Files.newInputStream(file.absolutePath)) {
			for (int partNumber = 1; partNumber <= numParts; partNumber++) {
				String partKey = String.valueOf(partNumber);
				if (!headerNode.has(partKey)) {
					throw new IOException("Missing URL for Part " + partNumber + " in header");
				}

				String partUrl = headerNode.get(partKey).asText();

				// 读取分片数据
				long bytesToRead = Math.min(chunkSize, file.size - ((partNumber - 1) * chunkSize));
				byte[] buffer = new byte[(int) bytesToRead];
				int read = is.read(buffer);
				if (read == -1)
					break;
				// 如果读取不满（文件结束），截断 buffer
				if (read < bytesToRead)
					buffer = Arrays.copyOf(buffer, read);

				// 上传分片
				HttpURLConnection partConn = (HttpURLConnection) new URL(partUrl).openConnection();
				partConn.setRequestMethod("PUT");
				partConn.setFixedLengthStreamingMode(buffer.length);
				partConn.setDoOutput(true);

				try (var os = partConn.getOutputStream()) {
					os.write(buffer);
				}

				int pCode = partConn.getResponseCode();
				if (pCode != 200) {
					String err = "";
					if (partConn.getErrorStream() != null)
						err = new String(partConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
					throw new IOException("LFS Part " + partNumber + " failed " + pCode + ": " + err);
				}

				// 获取 ETag
				String etag = partConn.getHeaderField("ETag");
				if (etag != null) {
					etag = etag.replace("\"", ""); // 去除引号
				} else {
					throw new IOException("ETag missing for Part " + partNumber);
				}

				uploadedParts.add(new PartInfo(partNumber, etag));
				logger.info(">>> [LFS Multipart] Part {}/{} uploaded (ETag: {})", partNumber, numParts, etag);
			}
		}

		// 完成分片上传
		// 文档 URL: POST /{repo}.git/info/lfs/complete/{upload_id}
		String completeUrl = String.format("%s/%s.git/info/lfs/complete/%s", baseUrl,
				URLEncoder.encode(repoId, StandardCharsets.UTF_8), uploadId);

		logger.info(">>> [LFS Multipart] Completing upload at: {}", completeUrl);

		HttpURLConnection completeConn = (HttpURLConnection) new URL(completeUrl).openConnection();
		completeConn.setRequestMethod("POST");
		completeConn.setRequestProperty("Content-Type", "application/json");
		completeConn.setRequestProperty("Authorization", "Bearer " + token); // 需要鉴权
		completeConn.setDoOutput(true);

		// 构建 Parts JSON
		StringBuilder partsJson = new StringBuilder("{\"oid\":\"").append(file.sha256).append("\",\"size\":")
				.append(file.size).append(",\"upload_id\":\"").append(uploadId).append("\",\"parts\":[");

		for (int i = 0; i < uploadedParts.size(); i++) {
			PartInfo p = uploadedParts.get(i);
			partsJson.append("{\"PartNumber\":").append(p.partNumber).append(",\"ETag\":\"").append(p.etag)
					.append("\"}");
			if (i < uploadedParts.size() - 1)
				partsJson.append(",");
		}
		partsJson.append("]}");

		try (var os = completeConn.getOutputStream()) {
			os.write(partsJson.toString().getBytes(StandardCharsets.UTF_8));
		}

		int cCode = completeConn.getResponseCode();
		if (cCode != 200) {
			String err = "";
			if (completeConn.getErrorStream() != null)
				err = new String(completeConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
			throw new IOException("LFS Complete failed " + cCode + ": " + err);
		}
		logger.info(">>> [LFS Multipart] Upload Complete Success.");
	}

	private void verifyLfsObject(String href, String token, LocalFile file) throws IOException {
		HttpURLConnection vConn = (HttpURLConnection) new URL(href).openConnection();
		vConn.setRequestMethod("POST");
		vConn.setRequestProperty("Content-Type", "application/json");
		vConn.setRequestProperty("Authorization", "Bearer " + token);
		vConn.setDoOutput(true);

		String body = String.format("{\"oid\":\"%s\",\"size\":%d}", file.sha256, file.size);
		vConn.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));

		int vCode = vConn.getResponseCode();
		if (vCode != 200) {
			String err = "";
			if (vConn.getErrorStream() != null)
				err = new String(vConn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
			logger.warn(">>> [LFS Verify] Failed ({}): {}", vCode, err);
		} else {
			logger.info(">>> [LFS Verify] Success.");
		}
	}

	private void commitChanges(String baseUrl, String token, String repoId, List<String> lines) throws IOException {
		String url = String.format("%s/api/models/%s/commit/main", baseUrl,
				URLEncoder.encode(repoId, StandardCharsets.UTF_8));
		HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Authorization", "Bearer " + token);
		conn.setRequestProperty("Content-Type", "application/x-ndjson");
		conn.setDoOutput(true);

		String payload = String.join("\n", lines);
		try (var os = conn.getOutputStream()) {
			os.write(payload.getBytes(StandardCharsets.UTF_8));
		}

		int code = conn.getResponseCode();
		if (code != 200) {
			String err = "";
			if (conn.getErrorStream() != null)
				err = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
			throw new IOException("Commit failed " + code + ": " + err);
		}
	}

	private String escape(String s) {
		return s.replace("\\", "\\\\").replace("\"", "\\\"");
	}

	// --- Static Helper Classes ---
	static class LocalFile {
		String relativePath;
		Path absolutePath;
		long size;
		String sha256;
	}

	static class PreuploadResult {
		LocalFile localFile;
		String uploadMode;
		boolean shouldIgnore;
	}

	static class PartInfo {
		int partNumber;
		String etag;

		PartInfo(int partNumber, String etag) {
			this.partNumber = partNumber;
			this.etag = etag;
		}
	}
}
