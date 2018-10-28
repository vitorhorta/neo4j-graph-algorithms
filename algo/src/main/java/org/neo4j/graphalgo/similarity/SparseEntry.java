package org.neo4j.graphalgo.similarity;

class SparseEntry {
    private final long item;
    private final long id;
    private final double weight;

    public SparseEntry(long item, long id, double weight) {
        this.item = item;
        this.id = id;
        this.weight = weight;
    }

    public long item() {
        return item;
    }

    public long id() {
        return id;
    }

    public double weight() {
        return weight;
    }

    @Override
    public String toString() {
        return "SparseEntry{" +
                "item=" + item +
                ", id=" + id +
                ", weight=" + weight +
                '}';
    }
}
