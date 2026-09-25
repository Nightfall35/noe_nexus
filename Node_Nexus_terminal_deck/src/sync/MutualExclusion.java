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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import ui.EventLog;

public class MutualExclusion {

    private static final Duration TOKEN_TIMEOUT = Duration.ofMillis(900);
    private static final Duration HEALTH_PROBE_TIMEOUT = Duration.ofMillis(700);
    // How long a peer that just failed a token delivery stays "known dead"
    // before we're willing to try sending it the token again. Keeping this
    // short-but-nonzero is the point of improvement #5: a single failed
    // hop shouldn't get retried on literally every subsequent token pass
    // (that just adds latency to every hop while the peer is down), but it
    // also shouldn't be permanent — the background health-probe loop below
    // clears it the moment the peer answers /api/health again.
    private static final long DEAD_COOLDOWN_MILLIS = 4000;
    private static final long HEALTH_PROBE_INTERVAL_MILLIS = 2000;

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

    // --- Failover bookkeeping (#5) --------------------------------------
    // Peer index -> the time (millis) until which we consider it dead and
    // won't attempt to send it the token. Populated on a failed delivery,
    // cleared either by the cooldown expiring or by the health-probe loop
    // confirming the peer answered /api/health again (whichever comes first).
    private final Map<Integer, Long> deadUntilMillis = new ConcurrentHashMap<>();
    private final AtomicLong tokenHopSuccessCount = new AtomicLong();
    private final AtomicLong tokenHopFailCount = new AtomicLong();
    // Incremented synchronously (before any async forwarding is even
    // dispatched) every time this node actually receives the token.
    // Exists mainly so tests/observability have a reliable "did the token
    // truly reach this node" signal -- unlike hasToken(), which can already
    // be false again a moment later once an idle node re-forwards.
    private final AtomicLong tokenReceivedCount = new AtomicLong();
    private final ScheduledExecutorService healthProbeScheduler = Executors.newSingleThreadScheduledExecutor();

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
        if (this.ringPeers.size() > 1) {
            healthProbeScheduler.scheduleAtFixedRate(this::probeDeadPeers,
                    HEALTH_PROBE_INTERVAL_MILLIS, HEALTH_PROBE_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }
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

    /** The STATIC ring neighbor (nodeId + 1) % size, regardless of any current failover. Contrast with {@link #getNextPeerAddress()}. */
    public String getConfiguredNextPeerAddress() {
        if (ringPeers.isEmpty()) return "-";
        return ringPeers.get((nodeId + 1) % ringPeers.size());
    }

    public long getTokenHopSuccessCount() {
        return tokenHopSuccessCount.get();
    }

    public long getTokenHopFailCount() {
        return tokenHopFailCount.get();
    }

    public long getTokenReceivedCount() {
        return tokenReceivedCount.get();
    }

    /** Peer indices currently considered dead (skipped for token delivery until they're probed healthy again or the cooldown expires). */
    public List<Integer> getDeadPeerIds() {
        long now = System.currentTimeMillis();
        return deadUntilMillis.entrySet().stream()
                .filter(e -> e.getValue() > now)
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
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
        tokenReceivedCount.incrementAndGet();
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
        List<Integer> dead = getDeadPeerIds();
        EventLog.writeTokenStatus(nodeId,
                "Node " + nodeId + "\n" +
                "Has token: " + hasToken + "\n" +
                "Waiting for CS: " + wantsToUpdateScore + "\n" +
                "Token state: " + tokenState + "\n" +
                "Status: " + status + "\n" +
                "Next peer in ring (active/failover): " + getNextPeerAddress() + "\n" +
                "Next peer in ring (configured/static): " + getConfiguredNextPeerAddress() + "\n" +
                "Currently skipped as dead: " + (dead.isEmpty() ? "none" : dead) + "\n" +
                "Token hops ok/failed: " + tokenHopSuccessCount.get() + "/" + tokenHopFailCount.get() + "\n");
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

    private boolean isKnownDead(int ringIndex) {
        Long until = deadUntilMillis.get(ringIndex);
        return until != null && until > System.currentTimeMillis();
    }

    private void markDead(int ringIndex, String address) {
        deadUntilMillis.put(ringIndex, System.currentTimeMillis() + DEAD_COOLDOWN_MILLIS);
        EventLog.appendTokenHop(nodeId, "FAILED delivery to node " + ringIndex + " (" + address
                + ") — marking dead for " + DEAD_COOLDOWN_MILLIS + "ms, will keep skipping until a health probe restores it.");
    }

    private void markRestored(int ringIndex, String address) {
        if (deadUntilMillis.remove(ringIndex) != null) {
            EventLog.appendTokenHop(nodeId, "RESTORED: node " + ringIndex + " (" + address + ") answered a health probe again.");
        }
    }

    /** Runs on a background schedule: pings every currently-dead peer's /api/health and un-marks it on success. */
    private void probeDeadPeers() {
        for (Integer ringIndex : getDeadPeerIds()) {
            String address = ringPeers.get(ringIndex);
            HttpRequest request;
            try {
                request = HttpRequest.newBuilder()
                        .uri(URI.create("http://" + address + "/api/health"))
                        .timeout(HEALTH_PROBE_TIMEOUT)
                        .GET()
                        .build();
            } catch (RuntimeException invalidAddress) {
                continue;
            }
            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response -> {
                        if (response.statusCode() >= 200 && response.statusCode() < 300) {
                            markRestored(ringIndex, address);
                        }
                    })
                    .exceptionally(ex -> null); // still dead; cooldown will naturally expire and we'll try a real delivery again
        }
    }

    private void sendTokenAttempt(String json, int attempt) {
        String address;
        int candidateIndex;
        synchronized (this) {
            if (attempt >= ringPeers.size()) {
                hasToken = true;
                tokenState = "STALLED -> no reachable peer";
                lastTokenChangeMillis = System.currentTimeMillis();
                writeTokenSnapshot();
                System.err.println("Node " + nodeId + ": token stalled; no configured peer is reachable");
                return;
            }
            candidateIndex = (successorIndex + attempt) % ringPeers.size();
            if (candidateIndex == nodeId) {
                sendTokenAttempt(json, attempt + 1);
                return;
            }
            if (isKnownDead(candidateIndex)) {
                // Known-dead: skip straight to the next candidate without
                // spending a real HTTP round-trip on a peer we already
                // know is down (that's the "avoid retrying known-dead
                // peers every token pass" improvement).
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

        int finalCandidateIndex = candidateIndex;
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    if (response.statusCode() >= 200 && response.statusCode() < 300) {
                        synchronized (this) {
                            successorIndex = finalCandidateIndex;
                            hasToken = false;
                            tokenState = "DELIVERED -> " + address;
                            lastTokenChangeMillis = System.currentTimeMillis();
                            writeTokenSnapshot();
                        }
                        markRestored(finalCandidateIndex, address); // a successful hop is the strongest possible "it's alive" signal
                        tokenHopSuccessCount.incrementAndGet();
                        EventLog.appendTokenHop(nodeId, "OK: token delivered to node " + finalCandidateIndex + " (" + address + ").");
                    } else {
                        tokenHopFailCount.incrementAndGet();
                        markDead(finalCandidateIndex, address);
                        sendTokenAttempt(json, attempt + 1);
                    }
                })
                .exceptionally(error -> {
                    tokenHopFailCount.incrementAndGet();
                    markDead(finalCandidateIndex, address);
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

    /** Stops the background health-probe thread. Call once, during graceful shutdown. */
    public void shutdown() {
        healthProbeScheduler.shutdownNow();
    }
}
