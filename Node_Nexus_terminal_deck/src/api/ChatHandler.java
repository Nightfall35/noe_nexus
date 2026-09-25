package api;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import models.Clock;
import models.Message;
import sync.Election;
import sync.MutualExclusion;
import ui.TypeFX;
import util.JsonUtil;

public class ChatHandler implements HttpHandler {

    private final int nodeId;
    private final int port;
    private final Clock clock;
    private final MutualExclusion mutex;
    private final Election election;
    private final NetworkClient networkClient;
    private final List<String> peerAddresses;
    private final String selfAddress;
    private final List<Message> messageLog = new ArrayList<>();
    private final long startTimeMillis = System.currentTimeMillis();

    /** Original constructor kept for compatibility; GUI features (send/status) are disabled without the extra args. */
    public ChatHandler(int nodeId, Clock clock, MutualExclusion mutex, Election election) {
        this(nodeId, -1, clock, mutex, election, null, null, null);
    }

    public ChatHandler(int nodeId, int port, Clock clock, MutualExclusion mutex, Election election,
                        NetworkClient networkClient, List<String> peerAddresses, String selfAddress) {
        this.nodeId = nodeId;
        this.port = port;
        this.clock = clock;
        this.mutex = mutex;
        this.election = election;
        this.networkClient = networkClient;
        this.peerAddresses = peerAddresses;
        this.selfAddress = selfAddress;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        // CORS: lets a browser-based dashboard (served from a different origin,
        // e.g. a local file or a hosted page) poll/control this node directly.
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");

        String method = exchange.getRequestMethod();
        if ("OPTIONS".equals(method)) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        try {
            String path = exchange.getRequestURI().getPath();

            if ("POST".equals(method) && "/api/chat".equals(path)) {
                handleChat(exchange);
            } else if ("POST".equals(method) && "/api/token".equals(path)) {
                handleToken(exchange);
            } else if ("POST".equals(method) && "/api/election".equals(path)) {
                handleElection(exchange);
            } else if ("GET".equals(method) && "/api/health".equals(path)) {
                sendResponse(exchange, 200, "{\"status\":\"ALIVE\"}");
            } else if ("GET".equals(method) && "/api/messages".equals(path)) {
                sendResponse(exchange, 200, serializeMessageLog());
            } else if ("GET".equals(method) && "/api/status".equals(path)) {
                sendResponse(exchange, 200, serializeStatus());
            } else if ("POST".equals(method) && "/api/send".equals(path)) {
                handleSend(exchange);
            } else if ("POST".equals(method) && "/api/action/score".equals(path)) {
                handleScoreAction(exchange);
            } else if ("POST".equals(method) && "/api/action/elect".equals(path)) {
                election.startElection();
                sendResponse(exchange, 200, "{\"status\":\"Election Forced\"}");
            } else {
                sendResponse(exchange, 404, "{\"error\":\"Not Found\"}");
            }
        } catch (Exception e) {
            System.err.println("Node " + nodeId + ": error handling request - " + e.getMessage());
            sendResponse(exchange, 400, "{\"error\":\"Bad Request\"}");
        }
    }

    /** GUI equivalent of typing plain text into the console: tick, log locally, broadcast to peers. */
    private void handleSend(HttpExchange exchange) throws IOException {
        String body = readBody(exchange);
        String text = extractString(body, "text", "");
        if (text.isBlank() || networkClient == null || peerAddresses == null) {
            sendResponse(exchange, 400, "{\"error\":\"send not available on this node\"}");
            return;
        }
        clock.tick();
        Message msg = new Message(nodeId, text, clock.getLamportTime(), clock.getVectorClock());
        synchronized (messageLog) {
            messageLog.add(msg);
            messageLog.sort(Message::compareForDisplay);
        }
        networkClient.broadcastChatMessage(nodeId, text, clock.getLamportTime(), clock.getVectorClock(), peerAddresses, selfAddress);
        sendResponse(exchange, 200, "{\"status\":\"sent\"}");
    }

    /** GUI equivalent of "/score <delta>": queue a delta and request the token. */
    private void handleScoreAction(HttpExchange exchange) throws IOException {
        String body = readBody(exchange);
        int delta = extractInt(body, "delta", 0);
        mutex.queueScoreUpdate(delta);
        mutex.requestCriticalSection();
        sendResponse(exchange, 200, "{\"status\":\"score requested\"}");
    }

    private String serializeStatus() {
        boolean isLeader = election.getCurrentLeaderId() == nodeId;
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"node_id\":").append(nodeId).append(",");
        sb.append("\"port\":").append(port).append(",");
        sb.append("\"leader_id\":").append(election.getCurrentLeaderId()).append(",");
        sb.append("\"is_leader\":").append(isLeader).append(",");
        sb.append("\"has_token\":").append(mutex.hasToken()).append(",");
        sb.append("\"waiting_for_token\":").append(mutex.isWaitingForToken()).append(",");
        sb.append("\"token_state\":\"").append(JsonUtil.escape(mutex.getTokenState())).append("\",");
        sb.append("\"token_next_peer\":\"").append(JsonUtil.escape(mutex.getNextPeerAddress())).append("\",");
        sb.append("\"token_passes\":").append(mutex.getTokenPasses()).append(",");
        sb.append("\"token_changed_at\":").append(mutex.getLastTokenChangeMillis()).append(",");
        sb.append("\"election_in_progress\":").append(election.isElectionInProgress()).append(",");
        sb.append("\"election_epoch\":").append(election.getCurrentEpoch()).append(",");
        sb.append("\"last_health_check_ms\":").append(election.getLastHealthCheckMillis()).append(",");
        sb.append("\"last_health_check_ok\":").append(election.isLastHealthCheckOk()).append(",");
        sb.append("\"uptime_ms\":").append(System.currentTimeMillis() - startTimeMillis).append(",");
        sb.append("\"token_next_peer_configured\":\"").append(JsonUtil.escape(mutex.getConfiguredNextPeerAddress())).append("\",");
        sb.append("\"token_hops_ok\":").append(mutex.getTokenHopSuccessCount()).append(",");
        sb.append("\"token_hops_failed\":").append(mutex.getTokenHopFailCount()).append(",");
        sb.append("\"dead_peers\":").append(mutex.getDeadPeerIds()).append(",");
        sb.append("\"send_ok\":").append(networkClient == null ? 0 : networkClient.getSendSuccessCount()).append(",");
        sb.append("\"send_failed\":").append(networkClient == null ? 0 : networkClient.getSendFailureCount()).append(",");
        sb.append("\"lamport\":").append(clock.getLamportTime()).append(",");
        sb.append("\"vector\":").append(Arrays.toString(clock.getVectorClock())).append(",");
        sb.append("\"scores\":{");
        Map<Integer, Integer> scores = new TreeMap<>(mutex.getScores());
        boolean first = true;
        for (Map.Entry<Integer, Integer> e : scores.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(e.getKey()).append("\":").append(e.getValue());
            first = false;
        }
        sb.append("},");
        sb.append("\"messages\":").append(serializeMessageLog());
        sb.append("}");
        return sb.toString();
    }

    private void handleChat(HttpExchange exchange) throws IOException {
        String body = readBody(exchange);
        int senderId = extractInt(body, "sender_id", -1);
        String text = extractString(body, "text", "");
        int incomingLamport = extractInt(body, "lamport", 0);
        int[] incomingVector = extractIntArray(body, "vector");

        clock.updateOnReceive(incomingLamport, incomingVector);

        Message msg = new Message(senderId, text, clock.getLamportTime(), clock.getVectorClock());
        synchronized (messageLog) {
            messageLog.add(msg);
            messageLog.sort(Message::compareForDisplay);
        }
        TypeFX.typeLine("  << [L" + msg.getLamportTime() + "] node" + senderId + ": " + text, TypeFX.CYAN);

        sendResponse(exchange, 200, "{\"status\":\"Message Received\"}");
    }

    private void handleToken(HttpExchange exchange) throws IOException {
        String body = readBody(exchange);
        Map<Integer, Integer> incomingScores = extractIntMap(body, "scores");
        mutex.mergeScores(incomingScores);
        mutex.receiveToken();
        sendResponse(exchange, 200, "{\"status\":\"Token Handled\"}");
    }

    private void handleElection(HttpExchange exchange) throws IOException {
        String body = readBody(exchange);
        String type = extractString(body, "type", "");
        int senderId = extractInt(body, "sender_id", -1);
        int epoch = extractInt(body, "epoch", 0);

        switch (type) {
            case "ELECTION" -> election.handleElectionMessage(senderId, epoch);
            case "COORDINATOR" -> election.handleCoordinatorMessage(senderId, epoch);
            case "OK" -> { /* no-op: our HTTP 200 response IS the OK — see Election.java */ }
            default -> System.err.println("Node " + nodeId + ": unknown election message type '" + type + "'");
        }

        sendResponse(exchange, 200, "{\"status\":\"OK\"}");
    }
    public List<Message> getRecentMessages(int count) {
        synchronized (messageLog) {
            int from = Math.max(0, messageLog.size() - count);
            return new ArrayList<>(messageLog.subList(from, messageLog.size()));
        }
    }

    private String serializeMessageLog() {
        StringBuilder sb = new StringBuilder("[");
        synchronized (messageLog) {
            for (int i = 0; i < messageLog.size(); i++) {
                if (i > 0) sb.append(",");
                Message m = messageLog.get(i);
                sb.append("{\"sender_id\":").append(m.getSenderId())
                  .append(",\"text\":\"").append(JsonUtil.escape(m.getText())).append("\"")
                  .append(",\"lamport\":").append(m.getLamportTime())
                  .append(",\"vector\":").append(Arrays.toString(m.getVectorClock()))
                  .append("}");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    private String readBody(HttpExchange exchange) throws IOException {
        return new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    private int extractInt(String json, String key, int defaultValue) {
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*(-?\\d+)").matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : defaultValue;
    }

    private String extractString(String json, String key, String defaultValue) {
        // (?:\\.|[^"\\])* — a run of "anything that isn't a quote or backslash,
        // or a backslash followed by any one character" — so an escaped quote
        // (\") inside the value no longer prematurely ends the match, and the
        // captured group still holds the raw (escaped) JSON text, which we
        // unescape below.
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*\"((?:\\\\.|[^\"\\\\])*)\"").matcher(json);
        return m.find() ? JsonUtil.unescape(m.group(1)) : defaultValue;
    }

    private int[] extractIntArray(String json, String key) {
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*\\[([^\\]]*)\\]").matcher(json);
        if (!m.find()) return new int[0];
        String inner = m.group(1).trim();
        if (inner.isEmpty()) return new int[0];
        String[] parts = inner.split(",");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Integer.parseInt(parts[i].trim());
        }
        return result;
    }

    private Map<Integer, Integer> extractIntMap(String json, String key) {
        Map<Integer, Integer> result = new HashMap<>();
        Matcher outer = Pattern.compile("\"" + key + "\"\\s*:\\s*\\{([^}]*)\\}").matcher(json);
        if (!outer.find()) return result;
        String inner = outer.group(1).trim();
        if (inner.isEmpty()) return result;
        Matcher entry = Pattern.compile("\"(-?\\d+)\"\\s*:\\s*(-?\\d+)").matcher(inner);
        while (entry.find()) {
            result.put(Integer.parseInt(entry.group(1)), Integer.parseInt(entry.group(2)));
        }
        return result;
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
