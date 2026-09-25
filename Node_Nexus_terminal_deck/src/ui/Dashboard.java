package ui;

import api.ChatHandler;
import models.Clock;
import models.Message;
import sync.Election;
import sync.MutualExclusion;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Dashboard {

    private static final int WIDTH = 78;
    private static final String RESET = "\u001B[0m";
    private static final String BOLD = "\u001B[1m";
    private static final String DIM = "\u001B[2m";
    private static final String CYAN = "\u001B[36m";
    private static final String BLUE = "\u001B[34m";
    private static final String YELLOW = "\u001B[33m";
    private static final String GREEN = "\u001B[32m";
    private static final String RED = "\u001B[31m";
    private static final String MAGENTA = "\u001B[35m";
    private static final String CLEAR_SCREEN = "\u001B[H\u001B[2J";

    private final int nodeId;
    private final int port;
    private final Clock clock;
    private final MutualExclusion mutex;
    private final Election election;
    private final ChatHandler chatHandler;
    private final long startTimeMillis = System.currentTimeMillis();

    public Dashboard(int nodeId, int port, Clock clock, MutualExclusion mutex,
                     Election election, ChatHandler chatHandler) {
        this.nodeId = nodeId;
        this.port = port;
        this.clock = clock;
        this.mutex = mutex;
        this.election = election;
        this.chatHandler = chatHandler;
    }

    public synchronized void renderToConsole() {
        System.out.print(CLEAR_SCREEN + build());
        System.out.flush();
    }

    public synchronized void writeToFile(Path path) {
        try {
            Files.writeString(path, build(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("Dashboard: failed to write status file " + path + " -> " + e.getMessage());
        }
    }

    private String build() {
        StringBuilder out = new StringBuilder();
        boolean leader = election.getCurrentLeaderId() == nodeId;
        String tokenState = mutex.getTokenState();
        String tokenColor = tokenState.startsWith("STALLED") ? RED
                : tokenState.startsWith("IN_FLIGHT") ? MAGENTA
                : tokenState.startsWith("DELIVERED") || tokenState.equals("RECEIVED") ? CYAN
                : mutex.hasToken() ? GREEN : DIM;

        out.append(divider('=')).append('\n');
        out.append(BOLD).append(CYAN).append("  NODE NEXUS :: LIVE NODE CONSOLE").append(RESET).append('\n');
        out.append("  node ").append(nodeId).append("  |  port ").append(port).append("  |  ")
           .append(GREEN).append("ONLINE").append(RESET).append("  |  ")
           .append(LocalTime.now().withNano(0)).append('\n');
        out.append(divider('=')).append('\n');

        out.append(sectionLabel("TOKEN RING  ")).append(tokenColor).append(tokenState).append(RESET).append('\n');
        out.append(tokenPath(tokenColor)).append('\n');
        out.append("  passes: ").append(mutex.getTokenPasses())
           .append("   next hop (active): ").append(mutex.getNextPeerAddress())
           .append("   last change: ").append(ageText(mutex.getLastTokenChangeMillis())).append('\n');
        out.append("  configured next hop: ").append(mutex.getConfiguredNextPeerAddress())
           .append("   hops ok/failed: ").append(GREEN).append(mutex.getTokenHopSuccessCount()).append(RESET)
           .append("/").append(mutex.getTokenHopFailCount() > 0 ? RED : DIM).append(mutex.getTokenHopFailCount()).append(RESET)
           .append("   skipped as dead: ").append(mutex.getDeadPeerIds().isEmpty() ? DIM + "none" + RESET : RED + mutex.getDeadPeerIds() + RESET)
           .append('\n');

        out.append(divider('-')).append('\n');
        out.append(sectionLabel("NODE STATUS")).append('\n');
        String leaderText = leader ? GREEN + "NODE " + nodeId + " (LEADER)" + RESET
                : "NODE " + election.getCurrentLeaderId();
        String tokenText = mutex.hasToken() ? GREEN + "HELD" + RESET
                : mutex.isWaitingForToken() ? YELLOW + "WAITING" + RESET : DIM + "NOT HELD" + RESET;
        String electionText = election.isElectionInProgress() ? YELLOW + "RUNNING" + RESET : DIM + "IDLE" + RESET;
        out.append("  leader: ").append(leaderText)
           .append("   token: ").append(tokenText)
           .append("   election: ").append(electionText)
           .append("   epoch: ").append(election.getCurrentEpoch()).append('\n');
        out.append("  lamport: ").append(clock.getLamportTime())
           .append("   vector: ").append(vectorToString(clock.getVectorClock())).append('\n');
        out.append("  uptime: ").append(uptimeText())
           .append("   last health check: ").append(election.isLastHealthCheckOk() ? GREEN + "OK" + RESET : RED + "FAILED" + RESET)
           .append(" (").append(ageText(election.getLastHealthCheckMillis())).append(')').append('\n');

        out.append(divider('-')).append('\n');
        out.append(sectionLabel("SCOREBOARD")).append('\n');
        out.append(scoreboardLines());

        out.append(divider('-')).append('\n');
        out.append(sectionLabel("CHAT STREAM  /  LAST 8")).append('\n');
        out.append(chatLogLines());

        out.append(divider('-')).append('\n');
        out.append(DIM).append("  refreshed ").append(LocalTime.now().withNano(0))
           .append("  |  token animation updates every second").append(RESET).append('\n');
        return out.toString();
    }

    private String tokenPath(String tokenColor) {
        String nextNode = extractNode(mutex.getNextPeerAddress());
        String left = mutex.hasToken() ? "●" : "○";
        String moving = tokenState(mutex.getTokenState(), "IN_FLIGHT") ? "●" : "·";
        String right = tokenState(mutex.getTokenState(), "DELIVERED") || tokenState(mutex.getTokenState(), "RECEIVED") ? "●" : "○";
        String arrow = tokenState(mutex.getTokenState(), "STALLED") ? "-X-" : "--->";
        return "  " + GREEN + "node" + nodeId + " [" + left + "]" + RESET
                + "  " + tokenColor + moving + "----" + arrow + "----" + RESET
                + "  " + CYAN + "node" + nextNode + " [" + right + "]" + RESET
                + "  " + DIM + "(ring continues)" + RESET;
    }

    private boolean tokenState(String state, String prefix) {
        return state.startsWith(prefix);
    }

    private String extractNode(String address) {
        int colon = address.lastIndexOf(':');
        if (colon < 0) return address;
        try {
            return String.valueOf((Integer.parseInt(address.substring(colon + 1)) - 8000));
        } catch (NumberFormatException e) {
            return address;
        }
    }

    private String ageText(long timestamp) {
        if (timestamp <= 0) return "never";
        long seconds = Math.max(0, Duration.ofMillis(System.currentTimeMillis() - timestamp).toSeconds());
        return seconds == 0 ? "now" : seconds + "s ago";
    }

    private String uptimeText() {
        long totalSeconds = Duration.ofMillis(System.currentTimeMillis() - startTimeMillis).toSeconds();
        long h = totalSeconds / 3600, m = (totalSeconds % 3600) / 60, s = totalSeconds % 60;
        return (h > 0 ? h + "h " : "") + (h > 0 || m > 0 ? m + "m " : "") + s + "s";
    }

    private String sectionLabel(String text) {
        return YELLOW + BOLD + text + RESET;
    }

    private String scoreboardLines() {
        Map<Integer, Integer> scores = new TreeMap<>(mutex.getScores());
        if (scores.isEmpty()) return "  " + DIM + "(no score updates yet)" + RESET + "\n";
        StringBuilder sb = new StringBuilder("  ");
        int count = 0;
        for (Map.Entry<Integer, Integer> entry : scores.entrySet()) {
            sb.append(BLUE).append("node").append(entry.getKey()).append(RESET)
              .append(": ").append(entry.getValue());
            if (++count < scores.size()) sb.append("     ");
        }
        return sb.append('\n').toString();
    }

    private String chatLogLines() {
        List<Message> recent = chatHandler.getRecentMessages(8);
        if (recent.isEmpty()) return "  " + DIM + "(no messages yet)" + RESET + "\n";
        StringBuilder sb = new StringBuilder();
        for (Message m : recent) {
            sb.append("  ").append(DIM).append("[L").append(m.getLamportTime()).append("]").append(RESET)
              .append(" ").append(CYAN).append("node").append(m.getSenderId()).append(RESET)
              .append(": ").append(m.getText()).append('\n');
        }
        return sb.toString();
    }

    private String vectorToString(int[] vector) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(vector[i]);
        }
        return sb.append(']').toString();
    }

    private String divider(char c) {
        return String.valueOf(c).repeat(WIDTH);
    }
}
