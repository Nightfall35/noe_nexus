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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Node {

    // Every node's address as "host:port", index = nodeId.
    // For all-on-one-machine testing, "localhost" is fine. To run across
    // MULTIPLE computers: replace "localhost" with each machine's real LAN
    // IP (e.g. "192.168.1.42:8000") for every node that isn't on THIS
    // machine, and use the same list, identically, on every machine —
    // everyone needs to agree on where everyone else lives. Find a
    // machine's LAN IP with `ipconfig` (Windows) or `ip addr` (Linux/Mac).
    private static final List<String> PEER_ADDRESSES = Arrays.asList(
        "localhost:8000", "localhost:8001", "localhost:8002", "localhost:8003", "localhost:8004",
        "localhost:8005", "localhost:8006", "localhost:8007", "localhost:8008", "localhost:8009"
    );

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java Node <nodeId> <port>");
            System.exit(1);
        }
        int nodeId = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);

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
        server.setExecutor(Executors.newCachedThreadPool());
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
