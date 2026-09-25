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
import java.util.concurrent.atomic.AtomicInteger;
import ui.EventLog;
import ui.TypeFX;

public class Election {

    private static final Duration ELECTION_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration HEALTH_CHECK_TIMEOUT = Duration.ofSeconds(1);

    private final int nodeId;
    private final List<String> peerAddresses; // index i holds node i's "host:port"
    private volatile int currentLeaderId;
    private volatile boolean isElectionInProgress = false;

    // --- Election epochs -----------------------------------------------
    // Every election this node RUNS gets a fresh, strictly increasing epoch
    // number. That epoch travels with the ELECTION and COORDINATOR messages
    // it produces. `acceptedEpoch` is the highest epoch this node has ever
    // accepted a result from (its own or someone else's). A message
    // carrying an epoch <= acceptedEpoch is necessarily from an election
    // that has already been superseded by a newer one — e.g. it arrived
    // late over a slow/retried connection — so it's stale and gets ignored
    // rather than being allowed to clobber a more recent result.
    private final AtomicInteger epochCounter = new AtomicInteger(0);
    private volatile int acceptedEpoch = 0;
    private volatile int myElectionEpoch = 0;

    private final HttpClient client = HttpClient.newHttpClient();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile long lastHealthCheckMillis = 0;
    private volatile boolean lastHealthCheckOk = true;

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
        int myEpoch = epochCounter.incrementAndGet();
        // A newly-booted node's own counter starts at 1, which could be
        // lower than an epoch this node has already witnessed from peers
        // (e.g. it rebooted mid-cluster-lifetime). Always run strictly
        // ahead of the highest epoch seen so far, so a legitimate new
        // election from a freshly-booted higher-ID node is never mistaken
        // for stale.
        if (myEpoch <= acceptedEpoch) {
            myEpoch = acceptedEpoch + 1;
            epochCounter.set(myEpoch);
        }
        myElectionEpoch = myEpoch;

        TypeFX.typeLine("  !! node " + nodeId + " starting election (epoch " + myEpoch + ")...", TypeFX.YELLOW);
        EventLog.appendElection(nodeId, "Starting election, epoch " + myEpoch + ".");
        isElectionInProgress = true;

        List<Integer> higherIds = new ArrayList<>();
        for (int id = nodeId + 1; id < peerAddresses.size(); id++) {
            higherIds.add(id);
        }

        if (higherIds.isEmpty()) {
            // Nobody outranks us — we win immediately, no need to wait.
            declareVictory(myEpoch);
            return;
        }

        AtomicBoolean higherNodeAlive = new AtomicBoolean(false);
        for (int higherId : higherIds) {
            sendElectionMessage(higherId, myEpoch, higherNodeAlive);
        }

        // Give higher nodes ELECTION_TIMEOUT to answer with an OK. If none
        // do, none of them are alive to contest, so we become leader.
        scheduler.schedule(() -> {
            // A newer election (ours or someone else's) may have already
            // superseded this one while we were waiting — don't declare
            // victory for a run that's already stale.
            if (myEpoch <= acceptedEpoch && myEpoch != myElectionEpoch) {
                return;
            }
            if (!higherNodeAlive.get()) {
                declareVictory(myEpoch);
            }
            // else: a higher node answered and is running its own election;
            // we now just wait for its COORDINATOR message (handleCoordinatorMessage).
        }, ELECTION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void sendElectionMessage(int targetId, int epoch, AtomicBoolean higherNodeAlive) {
        String json = "{\"type\":\"ELECTION\",\"sender_id\":" + nodeId + ",\"epoch\":" + epoch + "}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + peerAddresses.get(targetId) + "/api/election"))
                .timeout(ELECTION_TIMEOUT)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        // A 200 response here IS the "OK" reply — ChatHandler responding at
        // all (rather than the connection failing/timing out, or answering
        // with a non-2xx status) tells us the higher node is alive and
        // taking over the election. A non-200 is treated the same as
        // unreachable: no vote counted.
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    if (response.statusCode() >= 200 && response.statusCode() < 300) {
                        higherNodeAlive.set(true);
                    } else {
                        EventLog.appendElection(nodeId, "ELECTION to node " + targetId
                                + " got non-2xx status " + response.statusCode() + " — treated as no vote.");
                    }
                })
                .exceptionally(ex -> null); // unreachable node just casts no vote
    }

    private void declareVictory(int epoch) {
        currentLeaderId = nodeId;
        isElectionInProgress = false;
        acceptedEpoch = Math.max(acceptedEpoch, epoch);
        TypeFX.typeLine("  ## node " + nodeId + " WON THE ELECTION (epoch " + epoch + ") — broadcasting COORDINATOR...", TypeFX.BOLD_GREEN);
        EventLog.appendElection(nodeId, "WON the election (epoch " + epoch + "). Broadcasting COORDINATOR to all peers.");

        String json = "{\"type\":\"COORDINATOR\",\"sender_id\":" + nodeId + ",\"epoch\":" + epoch + "}";
        for (int id = 0; id < peerAddresses.size(); id++) {
            if (id == nodeId) continue;
            int targetId = id;
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + peerAddresses.get(id) + "/api/election"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();
            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response -> {
                        if (response.statusCode() < 200 || response.statusCode() >= 300) {
                            EventLog.appendElection(nodeId, "COORDINATOR to node " + targetId
                                    + " got non-2xx status " + response.statusCode() + ".");
                        }
                    })
                    .exceptionally(ex -> null); // dead peers just miss the announcement; they'll catch up on their next /health check or election
        }
    }

    public void handleElectionMessage(int senderId, int epoch) {
        if (epoch <= acceptedEpoch) {
            EventLog.appendElection(nodeId, "Ignoring stale ELECTION from node " + senderId
                    + " (epoch " + epoch + " <= accepted epoch " + acceptedEpoch + ").");
            return; // Our HTTP 200 still goes out — see ChatHandler — but we don't act on it.
        }

        // Our own HTTP 200 response to this POST (sent by ChatHandler) IS
        // the "OK" reply — the sender only outranked itself into calling us
        // because it saw we're alive, so answering at all settles that.
        TypeFX.typeLine("  !! ELECTION from node " + senderId + " (epoch " + epoch + ") — replying OK, contesting.", TypeFX.YELLOW);
        EventLog.appendElection(nodeId, "Received ELECTION from node " + senderId + " (epoch " + epoch + ") — replied OK, contesting.");

        // Bully rule: since sender only contacts higher IDs, we outrank it.
        // We must now contest the election ourselves (unless already doing so).
        if (!isElectionInProgress) {
            startElection();
        }
    }

    public void handleCoordinatorMessage(int newLeaderId, int epoch) {
        if (epoch < acceptedEpoch) {
            EventLog.appendElection(nodeId, "Ignoring stale COORDINATOR from node " + newLeaderId
                    + " (epoch " + epoch + " < accepted epoch " + acceptedEpoch + ").");
            return;
        }
        this.acceptedEpoch = epoch;
        this.currentLeaderId = newLeaderId;
        this.isElectionInProgress = false;
        TypeFX.typeLine("  ## new leader recognized: node " + newLeaderId + " (epoch " + epoch + ")", TypeFX.CYAN);
        EventLog.appendElection(nodeId, "New leader recognized: node " + newLeaderId + " (epoch " + epoch + ")");
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
                .thenAccept(response -> {
                    lastHealthCheckMillis = System.currentTimeMillis();
                    lastHealthCheckOk = response.statusCode() >= 200 && response.statusCode() < 300;
                    if (!lastHealthCheckOk) {
                        TypeFX.printInstant("  XX leader " + currentLeaderId + " health check returned " + response.statusCode() + " — starting election.", TypeFX.RED);
                        EventLog.appendElection(nodeId, "Leader " + currentLeaderId + " health check returned " + response.statusCode() + " — starting election.");
                        startElection();
                    }
                })
                .exceptionally(ex -> {
                    lastHealthCheckMillis = System.currentTimeMillis();
                    lastHealthCheckOk = false;
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

    public int getCurrentEpoch() {
        return acceptedEpoch;
    }

    public long getLastHealthCheckMillis() {
        return lastHealthCheckMillis;
    }

    public boolean isLastHealthCheckOk() {
        return lastHealthCheckOk;
    }

    /** Stops the internal scheduler thread. Call once, during graceful shutdown. */
    public void shutdown() {
        scheduler.shutdownNow();
    }
}
