package com.kohaku.manager.controller;

import com.kohaku.manager.model.TaskStatus;
import com.kohaku.manager.service.KohakuHubService;
import com.kohaku.manager.service.ModelScopeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
@RequestMapping("/api")
public class ManagerController {

    @Value("${cache.path:./model_cache}")
    private String cachePathConfig;

    @Autowired
    private ModelScopeService modelScopeService;

    @Autowired
    private KohakuHubService kohakuHubService;

    @GetMapping("/config")
    public Map<String, String> getConfig() {
        return Map.of("cachePath", cachePathConfig);
    }

    // 下载
    @PostMapping("/download/start")
    public ResponseEntity<Map<String, String>> startDownload(@RequestBody Map<String, String> request) {
        String modelName = request.get("modelName");
        if (modelName == null || modelName.isEmpty()) return ResponseEntity.badRequest().build();

        String taskId = UUID.randomUUID().toString();
        Path cacheDir = Paths.get(cachePathConfig);
        modelScopeService.downloadModel(taskId, modelName, cacheDir);

        return ResponseEntity.ok(Map.of("taskId", taskId));
    }

    // 获取本地模型列表
    @GetMapping("/models/local")
    public ResponseEntity<List<String>> getLocalModels() {
        Path cacheDir = Paths.get(cachePathConfig);
        if (!Files.exists(cacheDir)) {
            return ResponseEntity.ok(Collections.emptyList());
        }
        try (Stream<Path> stream = Files.list(cacheDir)) {
            List<String> models = stream
                    .filter(Files::isDirectory)
                    .map(p -> p.getFileName().toString())
                    .collect(Collectors.toList());
            return ResponseEntity.ok(models);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }
    // 辅助方法：安全转换 Long
    private long parseLong(Object value, long defaultValue) {
        if (value == null) return defaultValue;
        if (value instanceof Long) return (Long) value;
        if (value instanceof Integer) return ((Integer) value).longValue();
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }
    @PostMapping("/upload/start")
    public Map<String, String> startUpload(@RequestBody Map<String, Object> payload) {
        String taskId = java.util.UUID.randomUUID().toString();

        // 获取基础参数
        String modelName = (String) payload.get("modelName");
        String repoId = (String) payload.get("repoId");
        String apiUrl = (String) payload.get("apiUrl");
        String token = (String) payload.get("token");

        // 获取 LFS 配置参数 (处理 JSON 数字可能是 Integer 或 Long 的情况)
        long lfsThreshold = parseLong(payload.get("lfsThresholdBytes"), 10_000_000L);
        long multipartThreshold = parseLong(payload.get("multipartThresholdBytes"), 100_000_000L);
        long chunkSize = parseLong(payload.get("chunkSizeBytes"), 50_000_000L);

        // 获取缓存根目录
        Path cacheDir = Paths.get(cachePathConfig+File.separator+modelName);//pathConfig.getCacheDir();

        // 调用更新后的 Service 方法
        try {
			kohakuHubService.uploadModel(
			    taskId,
			    modelName,
			    cacheDir,
			    repoId,
			    apiUrl,
			    token,
			    lfsThreshold,
			    multipartThreshold,
			    chunkSize
			);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        Map<String, String> response = new HashMap<>();
        response.put("taskId", taskId);
        return response;
    }
    // 上传
    @PostMapping("/upload/start11")
    public ResponseEntity<Map<String, String>> startUpload11(@RequestBody Map<String, String> request) {
        String modelName = request.get("modelName");
        String apiUrl = request.get("apiUrl");
        String token = request.get("token");

        if (modelName == null || apiUrl == null) return ResponseEntity.badRequest().build();

        String taskId = UUID.randomUUID().toString();
        Path cacheDir = Paths.get(cachePathConfig);
        //kohakuHubService.uploadModel(taskId, modelName, cacheDir, apiUrl, token);
        //String taskId, String modelName, String cacheDirStr, String apiUrl, String token

        return ResponseEntity.ok(Map.of("taskId", taskId));
    }

    // 查询任务状态
    @GetMapping("/tasks")
    public Map<String, TaskStatus> getAllTasks() {
        Map<String, TaskStatus> all = new HashMap<>();
        all.putAll(modelScopeService.getAllTasks());
        all.putAll(kohakuHubService.getAllTasks());
        return all;
    }
}
