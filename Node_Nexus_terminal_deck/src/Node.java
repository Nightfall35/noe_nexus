import com.sun.net.httpserver.HttpServer;
import api.ChatHandler;
import api.NetworkClient;
import api.StaticDashboardHandler;
import models.Clock;
import sync.Election;
import sync.MutualExclusion;
import ui.Dashboard;
import ui.TypeFX;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Node {

    // Every node's address as "host:port", index = nodeId.
    // This list must be IDENTICAL — same order, same addresses — on every
    // machine in the cluster. It's compiled into the class, so after
    // editing this you need to recompile and copy the resulting classes
    // (or re-copy this .java file and rebuild) to BOTH machines below.
    //
    // node 0 = this machine (172.16.49.41)
    // node 1 = the other machine (172.21.54.149)
    private static final List<String> PEER_ADDRESSES = Arrays.asList(
        "172.16.49.41:8000", "172.21.54.149:8000"
    );

    /**
     * Fail fast with a clear message instead of starting with an
     * inconsistent configuration. Checked before anything else opens a
     * socket or spawns a thread.
     */
    static void validateTopology(int nodeId, int port, List<String> peerAddresses) {
        List<String> errors = new java.util.ArrayList<>();

        if (peerAddresses.isEmpty()) {
            errors.add("peer list is empty — at least one node address is required.");
        }
        if (nodeId < 0 || nodeId >= peerAddresses.size()) {
            errors.add("node ID " + nodeId + " is out of range for a peer list of size " + peerAddresses.size()
                    + " (valid IDs: 0.." + (peerAddresses.size() - 1) + ").");
        }

        // Duplicate node addresses / duplicate node IDs: the peer list is
        // indexed by node ID, so "no duplicate IDs" is structural (a List
        // can't have two entries at the same index) — the real risk is two
        // DIFFERENT indices pointing at the same address, which would make
        // two "different" nodes collide on the wire.
        Map<String, List<Integer>> addressToIds = new java.util.LinkedHashMap<>();
        for (int i = 0; i < peerAddresses.size(); i++) {
            addressToIds.computeIfAbsent(peerAddresses.get(i).toLowerCase(java.util.Locale.ROOT),
                    k -> new java.util.ArrayList<>()).add(i);
        }
        for (Map.Entry<String, List<Integer>> entry : addressToIds.entrySet()) {
            if (entry.getValue().size() > 1) {
                errors.add("duplicate address '" + entry.getKey() + "' is assigned to node IDs " + entry.getValue() + ".");
            }
        }

        // The local port must match what the compiled peer list says THIS
        // node ID lives at, or every other node will be sending messages to
        // an address nobody is listening on.
        if (nodeId >= 0 && nodeId < peerAddresses.size()) {
            String configured = peerAddresses.get(nodeId);
            int colon = configured.lastIndexOf(':');
            String configuredPortStr = colon >= 0 ? configured.substring(colon + 1) : "";
            try {
                int configuredPort = Integer.parseInt(configuredPortStr);
                if (configuredPort != port) {
                    errors.add("port mismatch: node " + nodeId + " was started with port " + port
                            + " but the peer list says node " + nodeId + " lives at '" + configured
                            + "' (port " + configuredPort + "). Every other node will try to reach you on the wrong port.");
                }
            } catch (NumberFormatException e) {
                errors.add("peer list entry for node " + nodeId + " ('" + configured + "') has no valid port.");
            }
        }

        // All nodes agreeing on the same topology size is guaranteed by
        // construction here (one hard-coded PEER_ADDRESSES list compiled
        // into every copy of this class) as long as every machine is
        // running the same build. This check exists mainly so a mismatched
        // deploy shows up in the boot banner rather than as mysterious
        // "unreachable peer" errors later.
        if (errors.isEmpty()) {
            TypeFX.printInstant(">> topology check OK — " + peerAddresses.size()
                    + " node(s) configured, this is node " + nodeId + " @ " + peerAddresses.get(nodeId), TypeFX.DIM);
        } else {
            TypeFX.printInstant("XX topology validation failed — refusing to start:", TypeFX.RED);
            for (String error : errors) {
                TypeFX.printInstant("   - " + error, TypeFX.RED);
            }
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java Node <nodeId> <port>");
            System.exit(1);
        }
        int nodeId = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);

        validateTopology(nodeId, port, PEER_ADDRESSES);

        int totalNodes = PEER_ADDRESSES.size();
        String selfAddress = PEER_ADDRESSES.get(nodeId);
        String nextPeerAddress = PEER_ADDRESSES.get((nodeId + 1) % totalNodes);

        Clock clock = new Clock(nodeId, totalNodes);
        MutualExclusion mutex = new MutualExclusion(nodeId, PEER_ADDRESSES, nodeId == 0);
        Election election = new Election(nodeId, PEER_ADDRESSES);
        NetworkClient networkClient = new NetworkClient();

        ChatHandler chatHandler = new ChatHandler(nodeId, port, clock, mutex, election,
                networkClient, PEER_ADDRESSES, selfAddress);
        // InetSocketAddress(port) (no host given) binds the WILDCARD address —
        // i.e. this server already listens on every network interface, not
        // just loopback, so it's reachable from other machines on the LAN
        // as soon as your firewall allows the port. No change needed here.
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/api", chatHandler);
        server.createContext("/", new StaticDashboardHandler(Path.of("dashboard.html")));
        // A cached thread pool (instead of null / the default single-threaded
        // executor) so an in-flight election or token POST from this node
        // doesn't block this node's own server from answering other requests
        // (e.g. a health check) at the same time.
        java.util.concurrent.ExecutorService httpExecutor = Executors.newCachedThreadPool();
        server.setExecutor(httpExecutor);
        server.start();

        // Periodically verify the current leader is still alive; triggers
        // Election.startElection() automatically if it isn't (see Election.java).
        ScheduledExecutorService healthScheduler = Executors.newSingleThreadScheduledExecutor();
        healthScheduler.scheduleAtFixedRate(election::checkLeaderHealth, 5, 5, TimeUnit.SECONDS);
        // Contest leadership immediately at boot rather than waiting on the first health check.
        election.startElection();

        Dashboard dashboard = new Dashboard(nodeId, port, clock, mutex, election, chatHandler);
        Path statusFile = Path.of("status_node" + nodeId + ".txt");

        // Write status to a FILE rather than clearing this process's own
        // screen. Open a second terminal pane (Windows Terminal: Alt+Shift+D)
        // in the same folder and run a poll loop to display it — see the
        // printed instructions below. That gives a true split terminal: this
        // pane stays a normal, undisturbed, scrolling input/log console, and
        // the second pane is purely live status, refreshed independently.
        ScheduledExecutorService renderScheduler = Executors.newSingleThreadScheduledExecutor();
        renderScheduler.scheduleAtFixedRate(() -> dashboard.writeToFile(statusFile), 0, 1, TimeUnit.SECONDS);

        // Graceful shutdown: on Ctrl+C (SIGINT) or normal JVM exit, stop
        // everything cleanly instead of leaving the port bound and threads
        // running until the OS reaps them.
        int shutdownNodeId = nodeId;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            TypeFX.printInstant("", TypeFX.DIM);
            TypeFX.printInstant(">> shutting down node " + shutdownNodeId + " ...", TypeFX.YELLOW);
            ui.EventLog.appendElection(shutdownNodeId, "Node shutting down (shutdown hook).");
            healthScheduler.shutdownNow();
            renderScheduler.shutdownNow();
            election.shutdown();
            mutex.shutdown();
            server.stop(1); // stop accepting new requests, allow 1s to drain in-flight ones
            httpExecutor.shutdown();
            try {
                if (!httpExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    httpExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                httpExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            TypeFX.printInstant(">> node " + shutdownNodeId + " stopped. port " + port + " released.", TypeFX.YELLOW);
        }, "shutdown-node" + nodeId));

        System.out.println();
        TypeFX.typeBanner(new String[] {
            "########################################",
            "#   N O D E _ N E X U S   v1.0        #",
            "#   distributed chat & scoreboard     #",
            "########################################"
        }, TypeFX.BOLD_GREEN);
        TypeFX.typeLine(">> booting node " + nodeId + " on port " + port + " ...", TypeFX.GREEN);
        TypeFX.typeLine(">> ring topology established, next peer -> " + nextPeerAddress, TypeFX.GREEN);
        TypeFX.typeLine(">> contesting leadership...", TypeFX.GREEN);
        TypeFX.typeLine(">> node online.", TypeFX.BOLD_GREEN);
        System.out.println();
        TypeFX.printInstant("Status is being written to " + statusFile.toAbsolutePath(), TypeFX.DIM);
        TypeFX.printInstant("GUI status endpoint: http://localhost:" + port + "/api/status  (CORS-enabled)", TypeFX.DIM);
        TypeFX.printInstant("For a live status pane, open a second terminal pane in this folder and run:", TypeFX.DIM);
        TypeFX.printInstant("  PowerShell:  while ($true) { Clear-Host; Get-Content " + statusFile + " -Raw; Start-Sleep -Seconds 1 }", TypeFX.DIM);
        TypeFX.printInstant("Type a message to broadcast it, '/score <n>' to bid for the token, or '/elect' to force an election.", TypeFX.CYAN);
        System.out.println();

        // Minimal console interface for manual/demo testing:
        //   plain text            -> broadcast as a chat message to all peers
        //   "/score <delta>"      -> request the token to update this node's score
        //   "/elect"              -> manually force a leader election
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = in.readLine()) != null) {
            if (line.isBlank()) {
                continue;
            } else if (line.startsWith("/score ")) {
                int delta = Integer.parseInt(line.substring("/score ".length()).trim());
                mutex.queueScoreUpdate(delta);
                mutex.requestCriticalSection();
                TypeFX.typeLine("  >> requested token to apply score delta " + delta, TypeFX.YELLOW);
            } else if (line.equals("/elect")) {
                election.startElection();
                TypeFX.typeLine("  >> forced a leader election", TypeFX.YELLOW);
            } else {
                clock.tick();
                networkClient.broadcastChatMessage(nodeId, line, clock.getLamportTime(), clock.getVectorClock(), PEER_ADDRESSES, selfAddress);
                TypeFX.typeLine("  >> sent", TypeFX.GREEN);
            }
            dashboard.writeToFile(statusFile);
        }
    }
}
