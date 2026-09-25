import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * ClusterLauncher — start (and cleanly stop) a whole local cluster with one command.
 *
 * Usage:
 *   java -cp out ClusterLauncher <nodeCount> [startPort]
 *
 * Example:
 *   java -cp out ClusterLauncher 10
 *   java -cp out ClusterLauncher 5 9000
 *
 * Each node runs as its own OS process (java -cp out Node <id> <port>),
 * exactly as if you'd started it by hand in its own terminal — this just
 * saves you from opening N terminals and keeping track of N PIDs yourself.
 * Each node's stdout/stderr is redirected to its own log file so nothing
 * is lost once the terminals are gone.
 */
public class ClusterLauncher {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java -cp out ClusterLauncher <nodeCount> [startPort]");
            System.exit(1);
        }

        int count = Integer.parseInt(args[0]);
        int startPort = args.length >= 2 ? Integer.parseInt(args[1]) : 8000;
        if (count < 1) {
            System.out.println("nodeCount must be at least 1.");
            System.exit(1);
        }

        String classpath = args.length >= 3 ? args[2] : "out";
        String javaBin = System.getProperty("java.home") + java.io.File.separator + "bin" + java.io.File.separator + "java";

        log("Node Nexus cluster launcher — starting " + count + " node(s), ports "
                + startPort + ".." + (startPort + count - 1) + " (classpath: " + classpath + ")");

        // --- Detect port collisions BEFORE spawning anything ------------
        // Catching this up front turns "node 4 mysteriously never comes up"
        // into a clear message before any process is even started.
        List<Integer> taken = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int port = startPort + i;
            if (!isPortFree(port)) {
                taken.add(port);
            }
        }
        if (!taken.isEmpty()) {
            log("XX Refusing to start: port(s) already in use: " + taken);
            log("   Stop whatever is using them (another cluster? a previous run that didn't shut down cleanly?) and try again.");
            System.exit(1);
        }
        log(">> Port check OK — all " + count + " port(s) are free.");

        // --- Spawn each node as its own process --------------------------
        List<Process> processes = new ArrayList<>();
        List<Path> logFiles = new ArrayList<>();
        boolean startFailed = false;
        for (int id = 0; id < count && !startFailed; id++) {
            int port = startPort + id;
            Path logFile = Path.of("launcher_node" + id + ".log");
            logFiles.add(logFile);
            ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", classpath, "Node", String.valueOf(id), String.valueOf(port));
            pb.redirectOutput(ProcessBuilder.Redirect.to(logFile.toFile()));
            pb.redirectErrorStream(true);
            try {
                Process p = pb.start();
                processes.add(p);
                log(">> node " + id + " started — pid " + p.pid() + ", port " + port + ", log -> " + logFile);
            } catch (IOException e) {
                log("XX failed to start node " + id + ": " + e.getMessage());
                startFailed = true;
            }
        }

        if (startFailed) {
            log("XX A node failed to start — stopping every node that DID start, to avoid a half-up cluster.");
            stopAll(processes);
            System.exit(1);
        }

        int nodeCount = count;
        // --- Stop the whole cluster cleanly on Ctrl+C or normal exit -----
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log(">> shutdown requested — stopping all " + nodeCount + " node(s)...");
            stopAll(processes);
            log(">> cluster stopped. Logs are in launcher_node0.log .. launcher_node" + (nodeCount - 1) + ".log");
        }, "cluster-shutdown"));

        log(">> cluster is up. " + count + " node(s) running. Press Ctrl+C to stop the whole cluster cleanly.");
        log(">> per-node logs: launcher_node0.log .. launcher_node" + (count - 1) + ".log");

        // Wait on all child processes so this launcher (and its shutdown
        // hook) stays alive for the life of the cluster; also surfaces an
        // unexpected node crash instead of silently leaving it dead.
        while (true) {
            boolean anyAlive = false;
            for (int id = 0; id < processes.size(); id++) {
                Process p = processes.get(id);
                if (p.isAlive()) {
                    anyAlive = true;
                } else if (!Boolean.TRUE.equals(reportedDeath.get(id))) {
                    reportedDeath.put(id, true);
                    log("XX node " + id + " exited unexpectedly (exit code " + p.exitValue()
                            + "). See " + logFiles.get(id) + " for details. Other nodes are left running.");
                }
            }
            if (!anyAlive) {
                log(">> all nodes have exited. Launcher shutting down.");
                break;
            }
            Thread.sleep(1000);
        }
    }

    private static final java.util.Map<Integer, Boolean> reportedDeath = new java.util.HashMap<>();

    private static boolean isPortFree(int port) {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("localhost", port));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static void stopAll(List<Process> processes) {
        // Ask nicely first (SIGTERM equivalent), give every node a moment
        // to run its own graceful-shutdown hook (closes its socket, flushes
        // logs), then force-kill anything still alive.
        for (Process p : processes) {
            if (p.isAlive()) {
                p.destroy();
            }
        }
        long deadline = System.currentTimeMillis() + 3000;
        for (Process p : processes) {
            long remaining = Math.max(0, deadline - System.currentTimeMillis());
            try {
                p.waitFor(remaining, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        for (Process p : processes) {
            if (p.isAlive()) {
                p.destroyForcibly();
            }
        }
    }

    private static void log(String message) {
        System.out.println("[" + LocalTime.now().format(TIME_FMT) + "] " + message);
    }
}
