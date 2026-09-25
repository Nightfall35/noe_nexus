package api;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import util.JsonUtil;


public class NetworkClient 
{

    private static final Duration CONNECT_TIMEOUT = Duration.ofMillis(750);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2);
    private static final boolean DEBUG_NETWORK = Boolean.getBoolean("node.debug.network");

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(CONNECT_TIMEOUT)
            .build();

    // Simple counters for the dashboard's diagnostics panel (#12): how many
    // outbound POSTs (chat broadcasts, etc.) have succeeded vs failed since
    // this node started.
    private final AtomicLong sendSuccessCount = new AtomicLong();
    private final AtomicLong sendFailureCount = new AtomicLong();

    public void broadcastChatMessage(int senderId, String text, int lamport, int[] vector,
                                      List<String> peerAddresses, String selfAddress) {
        String json = buildChatJson(senderId, text, lamport, vector);
        for (String address : peerAddresses) {
            if (address.equals(selfAddress)) continue;
            sendPost(address, "/api/chat", json);
        }
    }
    public void sendPost(String address, String path, String json) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address + path))
                .timeout(REQUEST_TIMEOUT)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    if (response.statusCode() >= 200 && response.statusCode() < 300) {
                        sendSuccessCount.incrementAndGet();
                    } else {
                        sendFailureCount.incrementAndGet();
                        if (DEBUG_NETWORK) {
                            System.err.println("NetworkClient: POST to " + address + path
                                    + " returned HTTP " + response.statusCode());
                        }
                    }
                })
                .exceptionally(ex -> {
                    sendFailureCount.incrementAndGet();
                    if (DEBUG_NETWORK) {
                        System.err.println("NetworkClient: failed POST to " + address + path
                                + " -> " + rootMessage(ex));
                    }
                    return null;
                });
    }

    public long getSendSuccessCount() {
        return sendSuccessCount.get();
    }

    public long getSendFailureCount() {
        return sendFailureCount.get();
    }

    private String rootMessage(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }

    private String buildChatJson(int senderId, String text, int lamport, int[] vector) {
        String escaped = JsonUtil.escape(text);
        return "{\"sender_id\":" + senderId
                + ",\"text\":\"" + escaped + "\""
                + ",\"lamport\":" + lamport
                + ",\"vector\":" + Arrays.toString(vector)
                + "}";
    }
}
