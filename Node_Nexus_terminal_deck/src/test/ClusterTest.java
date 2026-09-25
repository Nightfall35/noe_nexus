package test;

import api.ChatHandler;
import api.NetworkClient;
import com.sun.net.httpserver.HttpServer;
import models.Clock;
import sync.Election;
import sync.MutualExclusion;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Self-contained, dependency-free integration test suite for Node Nexus.
 *
 * There's no JUnit/Maven/Gradle in this project on purpose (see the
 * project notes), so this is a plain {@code public static void main} test
 * runner instead: each scenario spins up a handful of REAL nodes -- real
 * {@link HttpServer}s, real loopback sockets, the exact same classes
 * {@code Node.main()} wires together -- inside this one JVM process, talks
 * to them exactly the way separate {@code java Node <id> <port>} processes
 * would talk to each other over the network, and asserts on the outcome.
 * Every scenario gets its own fresh nodes and its own port range so
 * scenarios can never interfere with each other, even run back-to-back.
 *
 * Run with:
 *   javac -d out $(find src -name "*.java")
 *   java -cp out test.ClusterTest
 *
 * A non-zero exit code means at least one scenario failed.
 */
public class ClusterTest {

    private static int passed = 0;
    private static int failed = 0;
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    public static void main(String[] args) throws Exception {
        run("Chat message containing quotes is delivered intact", ClusterTest::testChatWithQuotes);
        run("Chat message containing newlines is delivered intact", ClusterTest::testChatWithNewlines);
        run("Token failover skips an unreachable peer (node 0 -> node 2, node 1 down)", ClusterTest::testTokenFailover);
        run("A recovered peer is un-marked dead and rejoins token delivery", ClusterTest::testTokenRecovery);
        run("Election succeeds when the highest-ID node is offline", ClusterTest::testElectionHighestOffline);
        run("Two nodes reachable at different-looking addresses can still talk", ClusterTest::testDifferentAddresses);
        run("A duplicate token delivery does not crash or wedge the node", ClusterTest::testDuplicateTokenDelivery);
        run("Malformed JSON body is handled gracefully, not a server crash", ClusterTest::testInvalidJson);
        run("An unreachable peer fails within a bounded time, it does not hang", ClusterTest::testHttpTimeoutBehavior);

        System.out.println();
        System.out.println("==============================================");
        System.out.println(" " + passed + " passed, " + failed + " failed");
        System.out.println("==============================================");
        if (failed > 0) System.exit(1);
    }

    private interface Scenario {
        void run() throws Exception;
    }

    private static void run(String name, Scenario scenario) {
        System.out.println(">> " + name);
        try {
            scenario.run();
            System.out.println("   PASS");
            passed++;
        } catch (AssertionError | Exception e) {
            System.out.println("   FAIL: " + e);
            failed++;
        }
    }

    // -----------------------------------------------------------------
    // Scenarios 1 & 2: quotes / newlines survive the hand-written JSON.
    // Regression coverage for the JsonUtil escaping fix.
    // -----------------------------------------------------------------

    private static void testChatWithQuotes() throws Exception {
        assertChatRoundTrips("She said \"hello\" to the node.", 20100);
    }

    private static void testChatWithNewlines() throws Exception {
        assertChatRoundTrips("line one\nline two\r\nline three", 20110);
    }

    private static void assertChatRoundTrips(String text, int startPort) throws Exception {
        List<String> peers = peerList(2, startPort);
        List<TestNode> nodes = startCluster(peers, -1);
        try {
            TestNode sender = nodes.get(0);
            sender.clock.tick();
            sender.networkClient.broadcastChatMessage(sender.id, text, sender.clock.getLamportTime(),
                    sender.clock.getVectorClock(), peers, peers.get(sender.id));

            String body = pollUntilContains(nodes.get(1).port, "\"text\"", Duration.ofSeconds(3));
            String extracted = extractJsonString(body, "text");
            assertEquals(text, extracted, "round-tripped chat text");
        } finally {
            stopAll(nodes);
        }
    }

    // -----------------------------------------------------------------
    // Scenario 3: token failover around a dead peer.
    // -----------------------------------------------------------------

    private static void testTokenFailover() throws Exception {
        List<String> peers = peerList(3, 20200); // 0 -> 1 -> 2 -> 0
        List<TestNode> nodes = startCluster(peers, 0); // node 0 starts holding the token
        try {
            TestNode n0 = nodes.get(0), n1 = nodes.get(1), n2 = nodes.get(2);
            n1.stop(); // node 1 is down BEFORE the token is ever sent
            Thread.sleep(200);

            n0.mutex.requestCriticalSection(); // node 0 already has the token -> applies + forwards immediately

            waitUntil(Duration.ofSeconds(4), () -> n2.mutex.getTokenReceivedCount() >= 1);
            assertTrue(n2.mutex.getTokenReceivedCount() >= 1,
                    "node 2 should have received the token after node 1 was skipped");
            assertTrue(n0.mutex.getTokenHopFailCount() >= 1,
                    "at least one hop attempt (to the dead node 1) should be recorded as failed");
        } finally {
            stopAll(nodes);
        }
    }

    // -----------------------------------------------------------------
    // Scenario 4: a peer that comes back online is un-marked dead.
    // -----------------------------------------------------------------

    private static void testTokenRecovery() throws Exception {
        List<String> peers = peerList(3, 20300);
        List<TestNode> nodes = startCluster(peers, 0);
        try {
            TestNode n0 = nodes.get(0), n1 = nodes.get(1), n2 = nodes.get(2);
            n1.stopServerOnly(); // "down", but the node object (and its state) still exists

            n0.mutex.requestCriticalSection();
            waitUntil(Duration.ofSeconds(4), () -> n2.mutex.getTokenReceivedCount() >= 1);
            assertTrue(n0.mutex.getDeadPeerIds().contains(1), "node 0 should have marked node 1 dead after a failed hop");

            n1.restartServer(); // node 1 "comes back online" on the same port

            // The background health-probe loop (and/or the dead-cooldown expiring) should
            // un-mark it within a few seconds -- give it enough cycles to do so.
            waitUntil(Duration.ofSeconds(6), () -> !n0.mutex.getDeadPeerIds().contains(1));
            assertTrue(!n0.mutex.getDeadPeerIds().contains(1),
                    "node 0 should un-mark node 1 dead once it is reachable again");
        } finally {
            stopAll(nodes);
        }
    }

    // -----------------------------------------------------------------
    // Scenario 5: election succeeds when the highest-ID node is offline.
    // -----------------------------------------------------------------

    private static void testElectionHighestOffline() throws Exception {
        List<String> peers = peerList(3, 20400); // ids 0,1,2 -> 2 is highest
        List<TestNode> nodes = startCluster(peers, -1);
        try {
            TestNode n0 = nodes.get(0), n1 = nodes.get(1), n2 = nodes.get(2);
            n2.stop(); // highest-ID node is offline for the whole scenario
            Thread.sleep(200);

            n0.election.startElection();

            waitUntil(Duration.ofSeconds(5),
                    () -> n0.election.getCurrentLeaderId() == 1 && n1.election.getCurrentLeaderId() == 1);
            assertEquals(1, n0.election.getCurrentLeaderId(), "node 0 should recognize node 1 as leader");
            assertEquals(1, n1.election.getCurrentLeaderId(), "node 1 should have elected itself leader");
        } finally {
            stopAll(nodes);
        }
    }

    // -----------------------------------------------------------------
    // Scenario 6: two nodes reachable at differently-styled addresses.
    // (One sandbox machine can't have two real NICs, but this still
    // proves nothing in the code path is hard-coded to the literal
    // string "localhost" -- arbitrary host strings are used verbatim,
    // which is exactly what makes a real two-PC deployment work.)
    // -----------------------------------------------------------------

    private static void testDifferentAddresses() throws Exception {
        int portA = 20500, portB = 20501;
        List<String> peers = List.of("127.0.0.1:" + portA, "localhost:" + portB);
        List<TestNode> nodes = startCluster(peers, -1);
        try {
            TestNode a = nodes.get(0);
            a.clock.tick();
            String text = "hello across address styles";
            a.networkClient.broadcastChatMessage(a.id, text, a.clock.getLamportTime(),
                    a.clock.getVectorClock(), peers, peers.get(a.id));
            String body = pollUntilContains(portB, "\"text\"", Duration.ofSeconds(3));
            assertEquals(text, extractJsonString(body, "text"),
                    "message from a '127.0.0.1' node reaching a 'localhost' node");
        } finally {
            stopAll(nodes);
        }
    }

    // -----------------------------------------------------------------
    // Scenario 7: duplicate token delivery.
    // Idempotent token IDs (dedup) were explicitly out of scope for this
    // change set -- this test documents/guards the current behavior: a
    // duplicate delivery must not crash or wedge the node, even though it
    // isn't deduplicated.
    // -----------------------------------------------------------------

    private static void testDuplicateTokenDelivery() throws Exception {
        List<String> peers = peerList(2, 20600);
        List<TestNode> nodes = startCluster(peers, -1);
        try {
            TestNode target = nodes.get(1);
            String tokenJson = "{\"token_holder\":0,\"scores\":{}}";
            int status1 = postRaw(target.port, "/api/token", tokenJson);
            int status2 = postRaw(target.port, "/api/token", tokenJson); // same token, sent twice back-to-back
            assertTrue(status1 >= 200 && status1 < 300, "first token delivery should succeed");
            assertTrue(status2 >= 200 && status2 < 300, "second (duplicate) token delivery should not error");

            // The real risk of a non-deduplicated duplicate isn't "wrong
            // balance" here (nobody requested the critical section) -- it's
            // the node getting confused or wedged. Confirm it's still alive.
            HttpRequest health = HttpRequest.newBuilder(URI.create("http://localhost:" + target.port + "/api/health"))
                    .timeout(Duration.ofSeconds(2)).GET().build();
            HttpResponse<String> resp = HTTP.send(health, HttpResponse.BodyHandlers.ofString());
            assertTrue(resp.statusCode() >= 200 && resp.statusCode() < 300,
                    "node should still answer /api/health after a duplicate token delivery");
        } finally {
            stopAll(nodes);
        }
    }

    // -----------------------------------------------------------------
    // Scenario 8: invalid JSON body.
    // -----------------------------------------------------------------

    private static void testInvalidJson() throws Exception {
        List<String> peers = peerList(1, 20700);
        List<TestNode> nodes = startCluster(peers, -1);
        try {
            TestNode node = nodes.get(0);
            int chatStatus = postRaw(node.port, "/api/chat", "{ this is not valid json !!");
            assertTrue(chatStatus < 500, "malformed /api/chat body should not 500-crash the handler (got " + chatStatus + ")");

            HttpRequest health = HttpRequest.newBuilder(URI.create("http://localhost:" + node.port + "/api/health"))
                    .timeout(Duration.ofSeconds(2)).GET().build();
            HttpResponse<String> resp = HTTP.send(health, HttpResponse.BodyHandlers.ofString());
            assertTrue(resp.statusCode() >= 200 && resp.statusCode() < 300,
                    "node should still answer /api/health after malformed input");
        } finally {
            stopAll(nodes);
        }
    }

    // -----------------------------------------------------------------
    // Scenario 9: an unreachable peer fails within a bounded time.
    // A guaranteed connection-refused port (nobody listening) is used
    // rather than an unroutable external IP, since this sandboxed
    // environment doesn't reliably reproduce a true network-level hang --
    // it exercises the identical bounded-timeout/exceptionally() code
    // path in MutualExclusion either way.
    // -----------------------------------------------------------------

    private static void testHttpTimeoutBehavior() throws Exception {
        int port0 = 20800, port1 = 20801; // port1: nobody is listening, on purpose
        List<String> peers = List.of("localhost:" + port0, "localhost:" + port1);
        TestNode n0 = new TestNode(0, peers, true);
        try {
            long start = System.currentTimeMillis();
            n0.mutex.requestCriticalSection(); // will try to forward to the nonexistent node 1 and fail
            waitUntil(Duration.ofSeconds(4), () -> n0.mutex.getTokenHopFailCount() >= 1);
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(n0.mutex.getTokenHopFailCount() >= 1, "the hop to the unreachable peer should be recorded as a failure");
            assertTrue(elapsed < 4000, "an unreachable peer should fail within a few seconds, not hang indefinitely (took " + elapsed + "ms)");
        } finally {
            n0.stop();
        }
    }

    // ===================================================================
    // Test infrastructure
    // ===================================================================

    private static List<String> peerList(int count, int startPort) {
        List<String> peers = new ArrayList<>();
        for (int i = 0; i < count; i++) peers.add("localhost:" + (startPort + i));
        return peers;
    }

    private static List<TestNode> startCluster(List<String> peers, int tokenHolderId) throws IOException {
        List<TestNode> nodes = new ArrayList<>();
        for (int i = 0; i < peers.size(); i++) {
            nodes.add(new TestNode(i, peers, i == tokenHolderId));
        }
        return nodes;
    }

    private static void stopAll(List<TestNode> nodes) {
        for (TestNode n : nodes) {
            try {
                n.stop();
            } catch (Exception ignored) {
                // best-effort cleanup between scenarios
            }
        }
    }

    private static int postRaw(int port, String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(2))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    private static String pollUntilContains(int port, String needle, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        String last = "";
        while (System.currentTimeMillis() < deadline) {
            HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/status"))
                    .timeout(Duration.ofSeconds(1)).GET().build();
            try {
                HttpResponse<String> resp = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
                last = resp.body();
                if (last.contains(needle)) return last;
            } catch (Exception ignored) {
                // server may not be ready yet -- keep polling until the deadline
            }
            Thread.sleep(150);
        }
        throw new AssertionError("timed out waiting for '" + needle + "' on port " + port + ". last body: " + last);
    }

    private static void waitUntil(Duration timeout, java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(100);
        }
        // Fall through -- the caller's own assertTrue(condition...) right
        // after this call will report the actual final state.
    }

    private static String extractJsonString(String json, String key) {
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*\"((?:\\\\.|[^\"\\\\])*)\"").matcher(json);
        if (!m.find()) throw new AssertionError("key '" + key + "' not found in: " + json);
        return util.JsonUtil.unescape(m.group(1));
    }

    private static void assertEquals(Object expected, Object actual, String what) {
        if (!java.util.Objects.equals(expected, actual)) {
            throw new AssertionError(what + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    private static void assertTrue(boolean condition, String what) {
        if (!condition) throw new AssertionError(what);
    }

    // ===================================================================
    // A single in-process node: a real HttpServer on a real loopback
    // socket, wired up from the exact same classes Node.main() uses.
    // ===================================================================
    private static class TestNode {
        final int id;
        final int port;
        final Clock clock;
        final MutualExclusion mutex;
        final Election election;
        final NetworkClient networkClient;
        final ChatHandler chatHandler;
        HttpServer server;
        boolean stopped = false;

        TestNode(int id, List<String> peers, boolean startsWithToken) throws IOException {
            this.id = id;
            this.port = portOf(peers.get(id));
            this.clock = new Clock(id, peers.size());
            this.mutex = new MutualExclusion(id, peers, startsWithToken);
            this.election = new Election(id, peers);
            this.networkClient = new NetworkClient();
            this.chatHandler = new ChatHandler(id, port, clock, mutex, election, networkClient, peers, peers.get(id));
            startServer();
        }

        private void startServer() throws IOException {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
            this.server.createContext("/api", chatHandler);
            this.server.setExecutor(Executors.newCachedThreadPool());
            this.server.start();
        }

        /** Simulates the node's process going down while KEEPING its in-memory state (used by the recovery scenario). */
        void stopServerOnly() {
            try {
                server.stop(0);
            } catch (Exception ignored) {
                // already stopped
            }
        }

        /** Simulates the node's process (or just its reachability) coming back, on the same port. */
        void restartServer() throws IOException {
            startServer();
        }

        /** Full teardown: server + background schedulers. Use when this node is done for the whole scenario. */
        void stop() {
            if (stopped) return;
            stopped = true;
            stopServerOnly();
            try {
                election.shutdown();
            } catch (Exception ignored) {
            }
            try {
                mutex.shutdown();
            } catch (Exception ignored) {
            }
        }
    }

    private static int portOf(String address) {
        return Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
    }
}
