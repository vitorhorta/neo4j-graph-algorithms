package org.neo4j.graphalgo;

public class SimilaritySummaryResult {

    public final long nodes;
    public final long similarityPairs;
    public final double percentile50;
    public final double percentile75;
    public final double percentile90;
    public final double percentile99;
    public final double percentile999;
    public final double percentile100;

    public SimilaritySummaryResult(long nodes, long similarityPairs,
                                   double percentile50, double percentile75, double percentile90, double percentile99,
                                   double percentile999, double percentile100) {
        this.nodes = nodes;
        this.similarityPairs = similarityPairs;
        this.percentile50 = percentile50;
        this.percentile75 = percentile75;
        this.percentile90 = percentile90;
        this.percentile99 = percentile99;
        this.percentile999 = percentile999;
        this.percentile100 = percentile100;
    }
}
