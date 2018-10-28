package org.neo4j.graphalgo.similarity;

public abstract class WeightedInput implements Comparable<WeightedInput> {
    final long id;

    public WeightedInput(long id) {
        this.id = id;
    }

    public int compareTo(WeightedInput o) {
        return Long.compare(id, o.id);
    }

    public abstract SimilarityResult cosineSquares(double cutoff, WeightedInput other);
}
