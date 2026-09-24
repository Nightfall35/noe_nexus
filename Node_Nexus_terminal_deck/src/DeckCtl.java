import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * One-shot companion to Deck: fires a single command at one node's HTTP API
 * and exits. Run this from a second terminal (Deck's screen redraws every
 * second, so it can't safely double as a place to type into).
 *
 * Usage:
 *   java DeckCtl <host:port> send "<message text>"
 *   java DeckCtl <host:port> score <delta>
 *   java DeckCtl <host:port> elect
 *
 * Example: java DeckCtl localhost:8003 send "hello from the deck"
 */
public class DeckCtl {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java DeckCtl <host:port> send \"<text>\" | score <delta> | elect");
            System.exit(1);
        }
        String address = args[0];
        String action = args[1];
        HttpClient client = HttpClient.newHttpClient();

        String path;
        String json;
        switch (action) {
            case "send" -> {
                if (args.length < 3) { System.out.println("send needs a message: java DeckCtl <host:port> send \"text\""); return; }
                String text = String.join(" ", java.util.Arrays.copyOfRange(args, 2, args.length));
                path = "/api/send";
                json = "{\"text\":\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}";
            }
            case "score" -> {
                if (args.length < 3) { System.out.println("score needs a delta: java DeckCtl <host:port> score <delta>"); return; }
                path = "/api/action/score";
                json = "{\"delta\":" + Integer.parseInt(args[2]) + "}";
            }
            case "elect" -> {
                path = "/api/action/elect";
                json = "{}";
            }
            default -> {
                System.out.println("Unknown action '" + action + "'. Use send, score, or elect.");
                return;
            }
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(response.statusCode() + " " + response.body());
    }
}
