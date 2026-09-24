import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * NODE_NEXUS control deck — a standalone, dependency-free terminal dashboard.
 *
 * Run this in its OWN terminal window (separate from any running Node
 * processes). It polls every node's GET /api/status once a second and
 * redraws a full-screen ANSI board: online/offline, leader, token, election,
 * Lamport/vector clocks, a merged scoreboard, and a merged chat log.
 *
 * This is intentionally read-only. Redrawing the whole screen every second
 * would blow away anything you're mid-typing on the same terminal, so
 * sending commands (chat / score bid / force election) is done from a
 * second terminal with DeckCtl, or from a node's own console as before.
 *
 * Usage:
 *   java Deck                          (localhost, ports 8000-8009)
 *   java Deck <host> <startPort> <count>
 */
public class Deck {

    private static final String RESET = "\u001B[0m";
    private static final String BOLD = "\u001B[1m";
    private static final String DIM = "\u001B[2m";
    private static final String GREEN = "\u001B[32m";
    private static final String BOLD_GREEN = "\u001B[1;32m";
    private static final String CYAN = "\u001B[36m";
    private static final String MAGENTA = "\u001B[35m";
    private static final String YELLOW = "\u001B[33m";
    private static final String RED = "\u001B[31m";
    private static final String CLEAR_HOME = "\u001B[H\u001B[2J";

    private static final int COLUMNS = 4;
    private static final int BOX_INNER = 26;
    private static final Duration TIMEOUT = Duration.ofMillis(900);

    private final HttpClient client = HttpClient.newHttpClient();
    private final String host;
    private final int startPort;
    private final int count;

    private final NodeState[] states;
    private final Deque<String> events = new ArrayDeque<>();
    private final Map<String, Message> seenMessages = new LinkedHashMap<>();

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int startPort = args.length > 1 ? Integer.parseInt(args[1]) : 8000;
        int count = args.length > 2 ? Integer.parseInt(args[2]) : 10;
        new Deck(host, startPort, count).run();
    }

    Deck(String host, int startPort, int count) {
        this.host = host;
        this.startPort = startPort;
        this.count = count;
        this.states = new NodeState[count];
        for (int i = 0; i < count; i++) states[i] = new NodeState(i, startPort + i);
    }

    void run() throws InterruptedException {
        event("control deck started, watching " + count + " node(s) at " + host + ":" + startPort + "-" + (startPort + count - 1));
        while (true) {
            pollAll();
            System.out.print(CLEAR_HOME + render());
            System.out.flush();
            Thread.sleep(1000);
        }
    }

    // ---------------------------------------------------------------- poll

    private void pollAll() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (NodeState s : states) {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + host + ":" + s.port + "/api/status"))
                    .timeout(TIMEOUT)
                    .GET().build();
            futures.add(client.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(res -> applyStatus(s, res.body()))
                    .exceptionally(ex -> {
                        markOffline(s);
                        return null;
                    }));
        }
        for (CompletableFuture<Void> f : futures) {
            try {
                f.get(TIMEOUT.toMillis() + 500, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {
                // treated as offline by the exceptionally() handler above
            }
        }
    }

    private void markOffline(NodeState s) {
        if (s.online) event("node " + s.id + " went offline / unreachable");
        s.online = false;
    }

    private void applyStatus(NodeState s, String json) {
        boolean wasOnline = s.online;
        try {
            s.online = true;
            s.leaderId = extractInt(json, "leader_id", -1);
            s.isLeader = extractBool(json, "is_leader");
            s.hasToken = extractBool(json, "has_token");
            s.waitingForToken = extractBool(json, "waiting_for_token");
            s.tokenState = extractString(json, "token_state", "UNKNOWN");
            s.tokenNextPeer = extractString(json, "token_next_peer", "-");
            s.tokenPasses = extractInt(json, "token_passes", 0);
            s.electionInProgress = extractBool(json, "election_in_progress");
            s.lamport = extractInt(json, "lamport", 0);
            s.vector = extractIntArray(json, "vector");
            s.scores = extractIntMap(json, "scores");
            List<Message> msgs = extractMessages(json);
            for (Message m : msgs) {
                seenMessages.putIfAbsent(m.sender + ":" + m.lamport + ":" + m.text, m);
            }
            if (!wasOnline) event("node " + s.id + " came online");
            if (wasOnline && s.leaderIdChanged()) event("node " + s.id + " now sees leader = node " + s.leaderId);
        } catch (Exception e) {
            markOffline(s);
        }
    }

    // -------------------------------------------------------------- render

    private String render() {
        StringBuilder out = new StringBuilder();
        out.append(BOLD_GREEN).append(center("N O D E _ N E X U S   ::   L I V E   C O N T R O L   D E C K", 4 + COLUMNS * (BOX_INNER + 4))).append(RESET).append('\n');
        out.append(DIM).append(center("watching " + host + ":" + startPort + "-" + (startPort + count - 1) + "   |   " + LocalTime.now().withNano(0), 4 + COLUMNS * (BOX_INNER + 4))).append(RESET).append("\n\n");

        out.append(renderGrid());
        out.append('\n');
        out.append(BOLD).append(YELLOW).append("SCOREBOARD (merged across nodes)").append(RESET).append('\n');
        out.append(renderScoreboard()).append('\n');
        out.append(BOLD).append(YELLOW).append("CHAT LOG (merged, last 10)").append(RESET).append('\n');
        out.append(renderChat()).append('\n');
        out.append(BOLD).append(YELLOW).append("EVENTS").append(RESET).append('\n');
        out.append(renderEvents());
        out.append('\n').append(DIM).append("read-only view — send commands with `java DeckCtl <host:port> send|score|elect ...` from another terminal").append(RESET).append('\n');
        return out.toString();
    }

    private String renderGrid() {
        StringBuilder out = new StringBuilder();
        for (int row = 0; row < count; row += COLUMNS) {
            List<List<String>> boxes = new ArrayList<>();
            int rowEnd = Math.min(row + COLUMNS, count);
            for (int i = row; i < rowEnd; i++) boxes.add(box(states[i]));
            int height = boxes.get(0).size();
            for (int line = 0; line < height; line++) {
                for (List<String> box : boxes) out.append(box.get(line)).append("  ");
                out.append('\n');
            }
        }
        return out.toString();
    }

    private List<String> box(NodeState s) {
        List<String> lines = new ArrayList<>();
        String border = "+" + "-".repeat(BOX_INNER + 2) + "+";
        lines.add(border);
        lines.add(cell(BOLD + CYAN + "NODE " + s.id + "  :: " + s.port + RESET));
        if (!s.online) {
            lines.add(cell(RED + "STATUS: OFFLINE" + RESET));
            lines.add(cell(DIM + "(no data)" + RESET));
            lines.add(cell(""));
            lines.add(cell(""));
            lines.add(cell(""));
            lines.add(cell(""));
        } else {
            lines.add(cell(GREEN + "STATUS: ONLINE" + RESET));
            String leader = s.isLeader ? YELLOW + "YOU (*)" + RESET : "node " + s.leaderId;
            lines.add(cell("LEADER: " + leader));
            String token = s.hasToken ? GREEN + "yes" + RESET : (s.waitingForToken ? YELLOW + "waiting" + RESET : DIM + "no" + RESET);
            String elec = s.electionInProgress ? YELLOW + "running" + RESET : DIM + "idle" + RESET;
            String route = s.tokenState.startsWith("IN_FLIGHT") ? MAGENTA + "moving" + RESET
                    : s.tokenState.startsWith("STALLED") ? RED + "stalled" + RESET : token;
            lines.add(cell("TOKEN: " + route + "  ELEC: " + elec));
            lines.add(cell("LAMPORT: " + s.lamport));
            lines.add(cell(CYAN + "NEXT: " + s.tokenNextPeer + RESET));
            lines.add(cell(DIM + "PASS: " + s.tokenPasses + RESET));
        }
        lines.add(border);
        return lines;
    }

    private String cell(String content) {
        return "| " + padRight(content, BOX_INNER) + " |";
    }

    private String renderScoreboard() {
        Map<Integer, Integer> merged = new TreeMap<>();
        for (NodeState s : states) if (s.online && s.scores != null) merged.putAll(s.scores);
        if (merged.isEmpty()) return DIM + "  (no score updates yet)" + RESET + "\n";
        StringBuilder sb = new StringBuilder("  ");
        int i = 0;
        for (Map.Entry<Integer, Integer> e : merged.entrySet()) {
            sb.append("node").append(e.getKey()).append(": ").append(e.getValue());
            if (++i < merged.size()) sb.append("     ");
        }
        return sb.append('\n').toString();
    }

    private String renderChat() {
        if (seenMessages.isEmpty()) return DIM + "  (no messages yet)" + RESET + "\n";
        List<Message> all = new ArrayList<>(seenMessages.values());
        all.sort((a, b) -> a.lamport != b.lamport ? Integer.compare(a.lamport, b.lamport) : Integer.compare(a.sender, b.sender));
        StringBuilder sb = new StringBuilder();
        int from = Math.max(0, all.size() - 10);
        for (Message m : all.subList(from, all.size())) {
            sb.append("  ").append(DIM).append("[L").append(m.lamport).append("]").append(RESET)
              .append(" ").append(CYAN).append("node").append(m.sender).append(RESET)
              .append(": ").append(m.text).append('\n');
        }
        return sb.toString();
    }

    private String renderEvents() {
        if (events.isEmpty()) return DIM + "  (none yet)" + RESET + "\n";
        StringBuilder sb = new StringBuilder();
        for (String e : events) sb.append("  ").append(DIM).append(e).append(RESET).append('\n');
        return sb.toString();
    }

    private void event(String text) {
        events.addFirst("[" + LocalTime.now().withNano(0) + "] " + text);
        while (events.size() > 8) events.removeLast();
    }

    // --------------------------------------------------------- text utils

    private static String visible(String s) {
        return s.replaceAll("\u001B\\[[;\\d]*m", "");
    }

    private static String padRight(String s, int width) {
        int len = visible(s).length();
        return len >= width ? s : s + " ".repeat(width - len);
    }

    private static String center(String s, int width) {
        int len = visible(s).length();
        if (len >= width) return s;
        int left = (width - len) / 2;
        return " ".repeat(Math.max(0, left)) + s;
    }

    private static String vectorString(int[] v) {
        if (v == null) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < v.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(v[i]);
        }
        return sb.append(']').toString();
    }

    // ------------------------------------------------------- tiny JSON bits
    // Hand-rolled, regex/scan based — matches the parsing style already used
    // in api/ChatHandler.java. No JSON library, on purpose.

    private static int extractInt(String json, String key, int def) {
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*(-?\\d+)").matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : def;
    }

    private static boolean extractBool(String json, String key) {
        Matcher m = Pattern.compile("\\\"" + key + "\\\"\\s*:\\s*(true|false)").matcher(json);
        return m.find() && Boolean.parseBoolean(m.group(1));
    }

    private static String extractString(String json, String key, String def) {
        Matcher m = Pattern.compile("\\\"" + key + "\\\"\\s*:\\s*\\\"([^\\\"]*)\\\"").matcher(json);
        return m.find() ? m.group(1) : def;
    }

    private static int[] extractIntArray(String json, String key) {
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*\\[([^\\]]*)\\]").matcher(json);
        if (!m.find()) return new int[0];
        String inner = m.group(1).trim();
        if (inner.isEmpty()) return new int[0];
        String[] parts = inner.split(",");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) result[i] = Integer.parseInt(parts[i].trim());
        return result;
    }

    private static Map<Integer, Integer> extractIntMap(String json, String key) {
        Map<Integer, Integer> result = new TreeMap<>();
        Matcher outer = Pattern.compile("\"" + key + "\"\\s*:\\s*\\{([^}]*)\\}").matcher(json);
        if (!outer.find()) return result;
        Matcher entry = Pattern.compile("\"(-?\\d+)\"\\s*:\\s*(-?\\d+)").matcher(outer.group(1));
        while (entry.find()) result.put(Integer.parseInt(entry.group(1)), Integer.parseInt(entry.group(2)));
        return result;
    }

    /** Scans the "messages":[ {...}, {...} ] array by brace-depth, so message text can safely contain , ] } etc. */
    private static List<Message> extractMessages(String json) {
        List<Message> out = new ArrayList<>();
        int idx = json.indexOf("\"messages\"");
        if (idx < 0) return out;
        int arrStart = json.indexOf('[', idx);
        if (arrStart < 0) return out;
        int i = arrStart + 1;
        while (i < json.length()) {
            while (i < json.length() && json.charAt(i) != '{' && json.charAt(i) != ']') i++;
            if (i >= json.length() || json.charAt(i) == ']') break;
            int objStart = i;
            int depth = 0;
            boolean inString = false;
            for (; i < json.length(); i++) {
                char c = json.charAt(i);
                if (inString) {
                    if (c == '\\') i++;
                    else if (c == '"') inString = false;
                } else if (c == '"') inString = true;
                else if (c == '{') depth++;
                else if (c == '}') {
                    depth--;
                    if (depth == 0) { i++; break; }
                }
            }
            String obj = json.substring(objStart, i);
            int sender = extractInt(obj, "sender_id", -1);
            int lamport = extractInt(obj, "lamport", 0);
            Matcher tm = Pattern.compile("\"text\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"").matcher(obj);
            String text = tm.find() ? tm.group(1).replace("\\\"", "\"").replace("\\\\", "\\") : "";
            out.add(new Message(sender, text, lamport));
        }
        return out;
    }

    private static class NodeState {
        final int id, port;
        boolean online = false;
        int leaderId = -1;
        boolean isLeader, hasToken, waitingForToken, electionInProgress;
        String tokenState = "UNKNOWN";
        String tokenNextPeer = "-";
        int tokenPasses;
        int lamport;
        int[] vector = new int[0];
        Map<Integer, Integer> scores = new TreeMap<>();
        private int prevLeaderId = -2;

        NodeState(int id, int port) { this.id = id; this.port = port; }

        boolean leaderIdChanged() {
            boolean changed = leaderId != prevLeaderId;
            prevLeaderId = leaderId;
            return changed;
        }
    }

    private static class Message {
        final int sender, lamport;
        final String text;
        Message(int sender, String text, int lamport) { this.sender = sender; this.text = text; this.lamport = lamport; }
    }
}
