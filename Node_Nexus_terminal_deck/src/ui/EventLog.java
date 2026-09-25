package ui;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalTime;

/**
 * Splits a node's output into separate files by category, so each can be
 * displayed in its own terminal pane (see the launcher script). Plain file
 * I/O only — no external logging library.
 */
public class EventLog {

    private static final Object LOCK = new Object();

    /** Appends a timestamped line — a running history — to election_node<id>.log. */
    public static void appendElection(int nodeId, String line) {
        append(Path.of("election_node" + nodeId + ".log"), line);
    }

    /** Overwrites token_node<id>.log with the current token/critical-section snapshot. */
    public static void writeTokenStatus(int nodeId, String content) {
        overwrite(Path.of("token_node" + nodeId + ".log"), content);
    }

    /** Appends a timestamped line recording one token hop's outcome (success or failure) to tokenhops_node<id>.log. */
    public static void appendTokenHop(int nodeId, String line) {
        append(Path.of("tokenhops_node" + nodeId + ".log"), line);
    }

    /** Overwrites scoreboard_node<id>.log with the current scoreboard snapshot. */
    public static void writeScoreboard(int nodeId, String content) {
        overwrite(Path.of("scoreboard_node" + nodeId + ".log"), content);
    }

    private static void append(Path path, String line) {
        String stamped = "[" + LocalTime.now().withNano(0) + "] " + line + System.lineSeparator();
        synchronized (LOCK) {
            try {
                Files.writeString(path, stamped, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.err.println("EventLog: failed to append to " + path + " -> " + e.getMessage());
            }
        }
    }

    private static void overwrite(Path path, String content) {
        synchronized (LOCK) {
            try {
                Files.writeString(path, content, StandardCharsets.UTF_8);
            } catch (IOException e) {
                System.err.println("EventLog: failed to write " + path + " -> " + e.getMessage());
            }
        }
    }
}
