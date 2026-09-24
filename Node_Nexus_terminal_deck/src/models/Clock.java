package models;

import java.util.Arrays;

public class Clock {
    private int lamportTime = 0;
    private final int[] vectorClock;
    private final int nodeId;

    public Clock(int nodeId, int totalNodes) {
        this.nodeId = nodeId;
        this.vectorClock = new int[totalNodes];
    }
    public synchronized void tick() {
        lamportTime++;
        vectorClock[nodeId]++;
    }
    public synchronized void updateOnReceive(int incomingLamport, int[] incomingVector) {
        lamportTime = Math.max(lamportTime, incomingLamport) + 1;
        for (int i = 0; i < vectorClock.length; i++) {
            vectorClock[i] = Math.max(vectorClock[i], incomingVector[i]);
        }
        vectorClock[nodeId]++;
    }

    public synchronized int getLamportTime() { return lamportTime; }

    public synchronized int[] getVectorClock() { return Arrays.copyOf(vectorClock, vectorClock.length); }
}
