package models;

import java.util.Arrays;


public class Message {
    private final int senderId;
    private final String text;
    private final int lamportTime;
    private final int[] vectorClock;

    public Message(int senderId, String text, int lamportTime, int[] vectorClock) {
        this.senderId = senderId;
        this.text = text;
        this.lamportTime = lamportTime;
        this.vectorClock = vectorClock;
    }

    public int getSenderId() { return senderId; }
    public String getText() { return text; }
    public int getLamportTime() { return lamportTime; }
    public int[] getVectorClock() { return vectorClock; }

   
    public static int compareForDisplay(Message a, Message b) {
        int cmp = Integer.compare(a.lamportTime, b.lamportTime);
        if (cmp != 0) return cmp;
        return Integer.compare(a.senderId, b.senderId);
    }

    @Override
    public String toString() {
        return "[L" + lamportTime + " V" + Arrays.toString(vectorClock) + "] node" + senderId + ": " + text;
    }
}
