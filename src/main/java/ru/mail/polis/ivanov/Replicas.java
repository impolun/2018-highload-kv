package ru.mail.polis.ivanov;

public class Replicas {
    private int ack, from;

    public Replicas(int ack, int from) {
        this.ack = ack;
        this.from = from;
    }

    public Replicas(String replicasIn){
        String[] replicas = replicasIn.split("/");
        ack = Integer.parseInt(replicas[0]);
        from = Integer.parseInt(replicas[1]);
    }

    public int getAck() {
        return ack;
    }

    public void setAck(int ack) {
        this.ack = ack;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    @Override
    public String toString() {
        return "Replicas{" +
                "ack=" + ack +
                ", from=" + from +
                '}';
    }
}