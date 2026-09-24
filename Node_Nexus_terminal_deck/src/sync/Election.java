package sync;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import ui.EventLog;
import ui.TypeFX;

public class Election {

    private static final Duration ELECTION_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration HEALTH_CHECK_TIMEOUT = Duration.ofSeconds(1);

    private final int nodeId;
    private final List<String> peerAddresses; // index i holds node i's "host:port"
    private volatile int currentLeaderId;
    private volatile boolean isElectionInProgress = false;

    private final HttpClient client = HttpClient.newHttpClient();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public Election(int nodeId, List<String> peerAddresses) {
        this.nodeId = nodeId;
        this.peerAddresses = peerAddresses;
        this.currentLeaderId = peerAddresses.size() - 1; // Highest ID is initial host
        EventLog.appendElection(nodeId, "Node " + nodeId + " booted. Assumed initial leader: node " + currentLeaderId);
    }

    public synchronized void startElection() {
        if (isElectionInProgress) {
            return; // Already running one — don't stack duplicate election storms
        }
        TypeFX.typeLine("  !! node " + nodeId + " starting election...", TypeFX.YELLOW);
        EventLog.appendElection(nodeId, "Starting election.");
        isElectionInProgress = true;

        List<Integer> higherIds = new ArrayList<>();
        for (int id = nodeId + 1; id < peerAddresses.size(); id++) {
            higherIds.add(id);
        }

        if (higherIds.isEmpty()) {
            // Nobody outranks us — we win immediately, no need to wait.
            declareVictory();
            return;
        }

        AtomicBoolean higherNodeAlive = new AtomicBoolean(false);
        for (int higherId : higherIds) {
            sendElectionMessage(higherId, higherNodeAlive);
        }

        // Give higher nodes ELECTION_TIMEOUT to answer with an OK. If none
        // do, none of them are alive to contest, so we become leader.
        scheduler.schedule(() -> {
            if (!higherNodeAlive.get()) {
                declareVictory();
            }
            // else: a higher node answered and is running its own election;
            // we now just wait for its COORDINATOR message (handleCoordinatorMessage).
        }, ELECTION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void sendElectionMessage(int targetId, AtomicBoolean higherNodeAlive) {
        String json = "{\"type\":\"ELECTION\",\"sender_id\":" + nodeId + "}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + peerAddresses.get(targetId) + "/api/election"))
                .timeout(ELECTION_TIMEOUT)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        // A 200 response here IS the "OK" reply — ChatHandler responding at
        // all (rather than the connection failing/timing out) tells us the
        // higher node is alive and taking over the election.
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    if (response.statusCode() == 200) {
                        higherNodeAlive.set(true);
                    }
                })
                .exceptionally(ex -> null); // unreachable node just casts no vote
    }

    private void declareVictory() {
        currentLeaderId = nodeId;
        isElectionInProgress = false;
        TypeFX.typeLine("  ## node " + nodeId + " WON THE ELECTION — broadcasting COORDINATOR...", TypeFX.BOLD_GREEN);
        EventLog.appendElection(nodeId, "WON the election. Broadcasting COORDINATOR to all peers.");

        String json = "{\"type\":\"COORDINATOR\",\"sender_id\":" + nodeId + "}";
        for (int id = 0; id < peerAddresses.size(); id++) {
            if (id == nodeId) continue;
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + peerAddresses.get(id) + "/api/election"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();
            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .exceptionally(ex -> null); // dead peers just miss the announcement; they'll catch up on their next /health check or election
        }
    }

    public void handleElectionMessage(int senderId) {
        // Our own HTTP 200 response to this POST (sent by ChatHandler) IS
        // the "OK" reply — the sender only outranked itself into calling us
        // because it saw we're alive, so answering at all settles that.
        TypeFX.typeLine("  !! ELECTION from node " + senderId + " — replying OK, contesting.", TypeFX.YELLOW);
        EventLog.appendElection(nodeId, "Received ELECTION from node " + senderId + " — replied OK, contesting.");

        // Bully rule: since sender only contacts higher IDs, we outrank it.
        // We must now contest the election ourselves (unless already doing so).
        if (!isElectionInProgress) {
            startElection();
        }
    }

    public void handleCoordinatorMessage(int newLeaderId) {
        this.currentLeaderId = newLeaderId;
        this.isElectionInProgress = false;
        TypeFX.typeLine("  ## new leader recognized: node " + newLeaderId, TypeFX.CYAN);
        EventLog.appendElection(nodeId, "New leader recognized: node " + newLeaderId);
    }

    /** Call periodically (e.g. every few seconds from Node.java) to detect a dead leader. */
    public void checkLeaderHealth() {
        if (currentLeaderId == nodeId) return; // we are the leader, nothing to check
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + peerAddresses.get(currentLeaderId) + "/api/health"))
                .timeout(HEALTH_CHECK_TIMEOUT)
                .GET()
                .build();
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .exceptionally(ex -> {
                    TypeFX.printInstant("  XX leader " + currentLeaderId + " unreachable — starting election.", TypeFX.RED);
                    EventLog.appendElection(nodeId, "Leader " + currentLeaderId + " unreachable (health check failed) — starting election.");
                    startElection();
                    return null;
                });
    }

    public synchronized int getCurrentLeaderId() {
        return currentLeaderId;
    }

    /** Whether an election is currently running — for dashboard/status display. */
    public boolean isElectionInProgress() {
        return isElectionInProgress;
    }
}
