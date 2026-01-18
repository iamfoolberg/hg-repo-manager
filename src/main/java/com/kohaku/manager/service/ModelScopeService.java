package com.kohaku.manager.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kohaku.manager.model.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ModelScopeService {
    private static final Logger logger = LoggerFactory.getLogger(ModelScopeService.class);
    private final ConcurrentHashMap<String, TaskStatus> tasks = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();

    // ModelScope 基础地址
    private static final String MODELSCOPE_BASE = "https://www.modelscope.cn";
    // API 列表接口地址 (这个接口是正确的，之前 Step 1 成功了)
    private static final String MODELSCOPE_API_BASE = "https://www.modelscope.cn/api/v1";

    public TaskStatus getTask(String id) {
        return tasks.get(id);
    }

    public ConcurrentHashMap<String, TaskStatus> getAllTasks() {
        return tasks;
    }

    @Async
    public void downloadModel(String taskId, String modelName, Path cacheDir) {
        TaskStatus status = new TaskStatus(taskId, "DOWNLOAD");
        tasks.put(taskId, status);

        // 本地缓存目录处理
        Path modelDir = cacheDir.resolve(modelName.replace("/", "_"));

        try {
            status.setMessage("正在连接 ModelScope 获取文件列表...");

            // 1. 获取文件列表 API (依然使用 api/v1 端点，因为这个接口有效)
            String listUrl = MODELSCOPE_API_BASE + "/models/" + modelName + "/repo/files?recurse=true";
            logger.info(">>> [Step 1] Requesting URL: {}", listUrl);

            HttpURLConnection conn = (HttpURLConnection) new URL(listUrl).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", "KohakuManager/1.0");
            conn.setRequestProperty("Accept", "application/json");

            if (conn.getResponseCode() != 200) {
                String errorMsg = "HTTP " + conn.getResponseCode();
                try (InputStream es = conn.getErrorStream()) {
                    if (es != null) errorMsg += ": " + new String(es.readAllBytes(), StandardCharsets.UTF_8);
                } catch (Exception ignored) {}
                throw new IOException("获取文件列表失败: " + errorMsg);
            }

            // 2. 解析 JSON 响应
            String rawResponse = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            JsonNode root = mapper.readTree(rawResponse);
            
            if (root.has("Data") && root.get("Data").isObject()) {
                JsonNode dataNode = root.get("Data");
                if (dataNode.has("Files") && dataNode.get("Files").isArray()) {
                    JsonNode filesArray = dataNode.get("Files");
                    
                    Files.createDirectories(modelDir);
                    int totalFiles = filesArray.size();
                    int processedFiles = 0;
                    int fileCount = 0;

                    logger.info(">>> [Step 2] Found {} items. Starting download...", totalFiles);

                    for (JsonNode fileNode : filesArray) {
                        String type = fileNode.has("Type") ? fileNode.get("Type").asText() : "unknown";
                        
                        if ("blob".equalsIgnoreCase(type)) {
                            fileCount++;
                            
                            // 获取 Revision 和 Path
                            String revision = fileNode.has("Revision") ? fileNode.get("Revision").asText() : "master";
                            String filePath = fileNode.has("Path") ? fileNode.get("Path").asText() : "";
                            
                            // 统一路径分隔符
                            filePath = filePath.replace("\\", "/");

                            if (filePath.isEmpty()) continue;

                            logger.info(">>> [Download] [{}/{}]: {} (Rev: {})", fileCount, totalFiles, filePath, revision);
                            status.setCurrentFile(filePath);
                            status.setMessage(String.format("正在下载 [%d/%d]: %s", fileCount, totalFiles, filePath));

                            try {
                                // 关键修正：使用浏览器直接访问的 URL 格式
                                downloadFile(modelName, revision, filePath, modelDir);
                                processedFiles++;
                                status.setProgress((processedFiles * 100) / totalFiles);
                            } catch (Exception e) {
                                logger.error(">>> [Error] Failed to download: {}", filePath, e);
                                throw new IOException("下载文件失败: " + filePath + " -> " + e.getMessage());
                            }
                        }
                    }

                    status.setStatus("COMPLETED");
                    status.setMessage("下载完成！共 " + processedFiles + " 个文件");
                    status.setCurrentFile("");
                    logger.info(">>> [Success] Model {} downloaded to {}", modelName, modelDir);
                    return;
                }
            }
            throw new IOException("API 返回格式异常，找不到 Data.Files 数组");

        } catch (Exception e) {
            logger.error(">>> [Fatal Error] Download failed for model {}", modelName, e);
            status.setStatus("ERROR");
            status.setMessage("错误: " + e.getMessage());
        }
    }

    private void downloadFile(String modelName, String revision, String filePath, Path targetDir) throws IOException {
        // 关键修正：使用 Web 兼容的 /resolve/ 路径，移除 /api/v1 前缀
        // 格式: https://www.modelscope.cn/models/{model_id}/resolve/{revision}/{path}
        // 注意：filePath 中的斜杠需要保留，不进行整体 URLEncoder
        
        // 为了安全起见，对 path 分段编码 (例如处理文件名中的空格)，这里简化处理
        // ModelScope 的路径通常是 URL 安全的
        String encodedPath = filePath; 
        
        String downloadUrl = MODELSCOPE_BASE + "/models/" + modelName + "/resolve/" + revision + "/" + encodedPath;

        logger.info(">>> [Requesting] {}", downloadUrl);

        HttpURLConnection conn = (HttpURLConnection) new URL(downloadUrl).openConnection();
        conn.setInstanceFollowRedirects(true);
        conn.setConnectTimeout(30000);
        conn.setReadTimeout(60000);
        // 必须伪装成浏览器，否则某些 CDN 或网关可能会拦截
        conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");
        conn.setRequestProperty("Accept", "*/*");

        try (InputStream in = conn.getInputStream()) {
            Path targetFile = targetDir.resolve(filePath);
            Files.createDirectories(targetFile.getParent());
            long size = Files.copy(in, targetFile, StandardCopyOption.REPLACE_EXISTING);
            logger.debug(">>> [Saved] {} bytes to {}", size, targetFile);
        } catch (IOException e) {
            String errorDetail = "";
            if (conn.getErrorStream() != null) {
                errorDetail = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            }
            logger.error(">>> [Download Error] HTTP {}: {}", conn.getResponseCode(), errorDetail);
            throw new IOException("HTTP " + conn.getResponseCode() + " (" + filePath + "): " + errorDetail);
        }
    }
}
