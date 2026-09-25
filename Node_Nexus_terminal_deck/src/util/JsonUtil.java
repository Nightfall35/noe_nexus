package util;

/**
 * Minimal shared JSON *string-value* escaping/unescaping.
 *
 * This project intentionally does not depend on a JSON library (see the
 * project notes on that trade-off). That's fine for the numeric/boolean
 * fields, but free-text chat messages need control characters (newline,
 * carriage return, tab) escaped or the hand-written JSON becomes invalid
 * the moment a user's message contains one. Centralizing that here means
 * every place that builds or reads a JSON string value (ChatHandler,
 * NetworkClient, MutualExclusion) agrees on the same encoding.
 */
public final class JsonUtil {

    private JsonUtil() {}

    /** Escapes a raw string for embedding as a JSON string value's contents (no surrounding quotes). */
    public static String escape(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"'  -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }

    /** Reverses {@link #escape(String)} — turns the raw matched contents of a JSON string back into text. */
    public static String unescape(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(++i);
                switch (next) {
                    case '\\' -> sb.append('\\');
                    case '"' -> sb.append('"');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case '/' -> sb.append('/');
                    case 'u' -> {
                        if (i + 4 < s.length()) {
                            String hex = s.substring(i + 1, i + 5);
                            try {
                                sb.append((char) Integer.parseInt(hex, 16));
                                i += 4;
                            } catch (NumberFormatException e) {
                                sb.append(next);
                            }
                        } else {
                            sb.append(next);
                        }
                    }
                    default -> sb.append(next);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
