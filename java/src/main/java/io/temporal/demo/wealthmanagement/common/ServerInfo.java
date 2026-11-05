package io.temporal.demo.wealthmanagement.common;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Settings read from the same environment variables as the Python components
 * (src/temporal_supervisor/model_config.py). The Temporal connection itself is configured in
 * application.yaml / application-tc.yaml.
 */
public final class ServerInfo {
    /** The Python worker's default main task queue (model_config.TASK_QUEUE). */
    public static final String DEFAULT_TASK_QUEUE = "wealth-management-strands";

    private ServerInfo() {
    }

    public static String getNamespace() {
        return getEnv("TEMPORAL_NAMESPACE", "default");
    }

    public static String getAddress() {
        return getEnv("TEMPORAL_ADDRESS", "127.0.0.1:7233");
    }

    /** The Python worker's task queue, where the activities this workflow calls run. */
    public static String getTaskqueue() {
        return getEnv("TEMPORAL_TASK_QUEUE", DEFAULT_TASK_QUEUE);
    }

    /** The task queue this worker polls for OpenInvestmentAccountWorkflow. */
    public static String getTaskqueueOpenAccount() {
        return getEnv("TEMPORAL_TASK_QUEUE_OPEN_ACCOUNT", getTaskqueue());
    }

    public static boolean useClaimCheck() {
        String value = getEnv("USE_CLAIM_CHECK", "false").trim().toLowerCase(Locale.ROOT);
        return value.equals("true") || value.equals("1") || value.equals("yes") || value.equals("on");
    }

    public static Map<String, String> getServerInfo() {
        Map<String, String> info = new LinkedHashMap<>();
        info.put("address", getAddress());
        info.put("namespace", getNamespace());
        info.put("taskQueue", getTaskqueue());
        info.put("taskQueueOpenAccount", getTaskqueueOpenAccount());
        info.put("useClaimCheck", String.valueOf(useClaimCheck()));
        return info;
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null && !value.isEmpty() ? value : defaultValue;
    }
}
