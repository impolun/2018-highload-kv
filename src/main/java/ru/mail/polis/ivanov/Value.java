package ru.mail.polis.ivanov;

import java.io.Serializable;
import java.util.Arrays;

public class Value implements Serializable {
    private final byte[] data;
    private final long timestamp;
    private final State state;

    enum State {
        PRESENT,
        DELETED,
        UNKNOWN
    }

    public Value(byte[] data) {
        this.data = data;
        this.timestamp = System.currentTimeMillis();
        this.state = State.PRESENT;
    }

    public Value(byte[] data, long timestamp, int state) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = State.values()[state];
    }

    public Value(byte[] data, long timestamp, State state) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = state;
    }

    public Value(byte[] data, long timestamp) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = State.PRESENT;
    }

    public State getState() {
        return state;
    }

    public byte[] getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Value{" +
                "data=" + Arrays.toString(data) +
                ", timestamp=" + timestamp +
                ", state=" + state +
                '}';
    }
}