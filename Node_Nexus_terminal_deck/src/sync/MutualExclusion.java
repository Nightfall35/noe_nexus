package sync;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import ui.EventLog;

public class MutualExclusion {

    private static final Duration TOKEN_TIMEOUT = Duration.ofMillis(900);

    private final int nodeId;
    private final List<String> ringPeers;
    private int successorIndex;
    private boolean wantsToUpdateScore = false;
    private boolean hasToken = false;
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(TOKEN_TIMEOUT)
            .build();
    private volatile String tokenState;
    private volatile long tokenPasses;
    private volatile long lastTokenChangeMillis;

    private final Map<Integer, Integer> scores = new ConcurrentHashMap<>();
    private volatile int pendingScoreDelta = 0;

    public MutualExclusion(int nodeId, String nextPeerAddress, boolean startsWithToken) {
        this(nodeId, List.of(nextPeerAddress), startsWithToken);
    }

    public MutualExclusion(int nodeId, List<String> peerAddresses, boolean startsWithToken) {
        this.nodeId = nodeId;
        this.ringPeers = List.copyOf(peerAddresses);
        this.successorIndex = this.ringPeers.isEmpty() ? -1 : (nodeId + 1) % this.ringPeers.size();
        this.hasToken = startsWithToken;
        this.tokenState = startsWithToken ? "HOLDING" : "IDLE";
        this.lastTokenChangeMillis = System.currentTimeMillis();
        writeTokenSnapshot();
        writeScoreboardSnapshot();
    }

    public synchronized void queueScoreUpdate(int delta) {
        this.pendingScoreDelta = delta;
    }

    public synchronized boolean hasToken() {
        return hasToken;
    }

    public synchronized boolean isWaitingForToken() {
        return wantsToUpdateScore;
    }

    public String getTokenState() {
        return tokenState;
    }

    public long getTokenPasses() {
        return tokenPasses;
    }

    public long getLastTokenChangeMillis() {
        return lastTokenChangeMillis;
    }

    public synchronized String getNextPeerAddress() {
        if (ringPeers.isEmpty() || successorIndex < 0) return "-";
        return ringPeers.get(successorIndex);
    }

    public synchronized void requestCriticalSection() {
        this.wantsToUpdateScore = true;
        writeTokenSnapshot();
        if (hasToken) {
            executeCriticalSectionAndForward();
        }
    }

    public synchronized void receiveToken() {
        hasToken = true;
        tokenState = "RECEIVED";
        lastTokenChangeMillis = System.currentTimeMillis();
        writeTokenSnapshot();
        if (wantsToUpdateScore) {
            executeCriticalSectionAndForward();
        } else {
            passToken();
        }
    }

    private void executeCriticalSectionAndForward() {
        scores.merge(nodeId, pendingScoreDelta, Integer::sum);
        writeScoreboardSnapshot();
        pendingScoreDelta = 0;
        wantsToUpdateScore = false;
        writeTokenSnapshot();
        passToken();
    }

    public Map<Integer, Integer> getScores() {
        return Map.copyOf(scores);
    }

    public synchronized void mergeScores(Map<Integer, Integer> incoming) {
        if (incoming != null) {
            scores.putAll(incoming);
            writeScoreboardSnapshot();
        }
    }

    private void writeTokenSnapshot() {
        String status = hasToken
                ? "Holding the token." + (wantsToUpdateScore ? " Applying queued score update now." : " Nothing to do, will forward.")
                : (wantsToUpdateScore ? "Waiting for the token to arrive (score update queued)." : "Idle, does not hold the token.");
        EventLog.writeTokenStatus(nodeId,
                "Node " + nodeId + "\n" +
                "Has token: " + hasToken + "\n" +
                "Waiting for CS: " + wantsToUpdateScore + "\n" +
                "Token state: " + tokenState + "\n" +
                "Status: " + status + "\n" +
                "Next peer in ring: " + getNextPeerAddress() + "\n");
    }

    private void writeScoreboardSnapshot() {
        Map<Integer, Integer> sorted = new TreeMap<>(scores);
        StringBuilder sb = new StringBuilder("Scoreboard (as known by node " + nodeId + ")\n");
        if (sorted.isEmpty()) {
            sb.append("  (no updates yet)\n");
        } else {
            for (Map.Entry<Integer, Integer> entry : sorted.entrySet()) {
                sb.append("  Node ").append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
            }
        }
        EventLog.writeScoreboard(nodeId, sb.toString());
    }

    private synchronized void passToken() {
        if (ringPeers.size() <= 1) {
            tokenState = "STALLED -> no other configured peer";
            hasToken = true;
            lastTokenChangeMillis = System.currentTimeMillis();
            writeTokenSnapshot();
            return;
        }
        String json = buildTokenJson();
        tokenPasses++;
        lastTokenChangeMillis = System.currentTimeMillis();
        sendTokenAttempt(json, 0);
    }

    private void sendTokenAttempt(String json, int attempt) {
        String address;
        synchronized (this) {
            if (attempt >= ringPeers.size()) {
                hasToken = true;
                tokenState = "STALLED -> no reachable peer";
                lastTokenChangeMillis = System.currentTimeMillis();
                writeTokenSnapshot();
                System.err.println("Node " + nodeId + ": token stalled; no configured peer is reachable");
                return;
            }
            int candidateIndex = (successorIndex + attempt) % ringPeers.size();
            if (candidateIndex == nodeId) {
                sendTokenAttempt(json, attempt + 1);
                return;
            }
            address = ringPeers.get(candidateIndex);
            tokenState = "IN_FLIGHT -> " + address;
            lastTokenChangeMillis = System.currentTimeMillis();
            writeTokenSnapshot();
        }

        HttpRequest request;
        try {
            request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + address + "/api/token"))
                    .timeout(TOKEN_TIMEOUT)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();
        } catch (RuntimeException invalidAddress) {
            sendTokenAttempt(json, attempt + 1);
            return;
        }

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    if (response.statusCode() >= 200 && response.statusCode() < 300) {
                        synchronized (this) {
                            successorIndex = (successorIndex + attempt) % ringPeers.size();
                            hasToken = false;
                            tokenState = "DELIVERED -> " + address;
                            lastTokenChangeMillis = System.currentTimeMillis();
                            writeTokenSnapshot();
                        }
                    } else {
                        sendTokenAttempt(json, attempt + 1);
                    }
                })
                .exceptionally(error -> {
                    sendTokenAttempt(json, attempt + 1);
                    return null;
                });
    }

    private String buildTokenJson() {
        StringBuilder scoresJson = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<Integer, Integer> entry : scores.entrySet()) {
            if (!first) scoresJson.append(",");
            scoresJson.append("\"").append(entry.getKey()).append("\":").append(entry.getValue());
            first = false;
        }
        scoresJson.append("}");
        return "{\"token_holder\":" + nodeId + ",\"scores\":" + scoresJson + "}";
    }
}
