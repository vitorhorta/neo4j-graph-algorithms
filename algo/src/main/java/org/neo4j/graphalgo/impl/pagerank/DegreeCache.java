package org.neo4j.graphalgo.impl.pagerank;

public class DegreeCache {

    public final static DegreeCache EMPTY = new DegreeCache(new double[0]);

    private double[] aggregatedDegrees;

    public DegreeCache(double[] aggregatedDegrees) {
        this.aggregatedDegrees = aggregatedDegrees;
    }

    public double[] aggregatedDegrees() {
        return aggregatedDegrees;
    }
}
