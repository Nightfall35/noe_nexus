package ui;

/**
 * Small "hacker terminal" text-effect utility: prints strings one character
 * at a time with a short delay, in color, instead of dumping the whole line
 * at once. Pure Java, no dependencies — just Thread.sleep and ANSI codes.
 *
 * Synchronized on a shared lock because output can come from multiple
 * threads at once (the HTTP handler thread printing an incoming chat
 * message, the console thread printing your own feedback, a scheduler
 * thread printing an election result) — without that, two typewriters
 * running at the same time would interleave their characters into garbage.
 */
public class TypeFX {

    private static final Object LOCK = new Object();

    public static final String RESET = "\u001B[0m";
    public static final String GREEN = "\u001B[32m";
    public static final String BOLD_GREEN = "\u001B[1;32m";
    public static final String CYAN = "\u001B[36m";
    public static final String YELLOW = "\u001B[33m";
    public static final String RED = "\u001B[31m";
    public static final String DIM = "\u001B[2m";

    /** Flip to false to make every call below print instantly (no delay) — handy while debugging. */
    public static boolean enabled = true;

    private static final int MS_PER_CHAR = 12;

    /** Types text in the given color, no trailing newline. */
    public static void type(String text, String color) {
        synchronized (LOCK) {
            System.out.print(color);
            if (!enabled) {
                System.out.print(text);
            } else {
                for (char c : text.toCharArray()) {
                    System.out.print(c);
                    System.out.flush();
                    sleep(c == ' ' ? MS_PER_CHAR / 3 : MS_PER_CHAR);
                }
            }
            System.out.print(RESET);
            System.out.flush();
        }
    }

    /** Types text in the given color, then a newline. */
    public static void typeLine(String text, String color) {
        type(text, color);
        System.out.println();
    }

    /** Types each line of a banner/ASCII-art block in sequence, in the given color. */
    public static void typeBanner(String[] lines, String color) {
        for (String line : lines) {
            typeLine(line, color);
        }
    }

    /** Prints instantly, no delay — for errors/warnings that shouldn't feel slow to notice. */
    public static void printInstant(String text, String color) {
        synchronized (LOCK) {
            System.out.println(color + text + RESET);
        }
    }

    private static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
