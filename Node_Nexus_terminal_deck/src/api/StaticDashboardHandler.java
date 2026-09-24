package api;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.net.URISyntaxException;

public class StaticDashboardHandler implements HttpHandler {

    private final Path dashboardPath;

    public StaticDashboardHandler(Path dashboardPath) {
        this.dashboardPath = dashboardPath;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if ("OPTIONS".equals(method)) {
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS");
            exchange.sendResponseHeaders(204, -1);
            return;
        }
        if (!"GET".equals(method)) {
            send(exchange, 405, "Method Not Allowed", "text/plain; charset=utf-8");
            return;
        }
        Path resolvedPath = resolveDashboardPath();
        if (resolvedPath == null) {
            send(exchange, 404, "dashboard.html not found", "text/plain; charset=utf-8");
            return;
        }
        byte[] content = Files.readAllBytes(resolvedPath);
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(200, content.length);
        exchange.getResponseBody().write(content);
        exchange.close();
    }

    private void send(HttpExchange exchange, int status, String body, String contentType) throws IOException {
        byte[] content = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, content.length);
        exchange.getResponseBody().write(content);
        exchange.close();
    }

    private Path resolveDashboardPath() {
        Path[] candidates = new Path[] {
                dashboardPath,
                Paths.get("..", "dashboard.html"),
                Paths.get("..", "..", "dashboard.html")
        };
        for (Path candidate : candidates) {
            if (Files.isRegularFile(candidate)) return candidate;
        }
        try {
            Path classesDirectory = Paths.get(StaticDashboardHandler.class
                    .getProtectionDomain().getCodeSource().getLocation().toURI());
            Path[] compiledCandidates = new Path[] {
                    classesDirectory.resolve("dashboard.html"),
                    classesDirectory.resolveSibling("dashboard.html"),
                    classesDirectory.resolve("..\u002fdashboard.html").normalize()
            };
            for (Path candidate : compiledCandidates) {
                if (Files.isRegularFile(candidate)) return candidate;
            }
        } catch (URISyntaxException ignored) {
        }
        return null;
    }
}
