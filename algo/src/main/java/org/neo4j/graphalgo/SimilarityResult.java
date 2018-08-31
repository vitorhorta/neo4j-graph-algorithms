package org.neo4j.graphalgo;

import java.util.Objects;

public class SimilarityResult {
    public final long source2;
    public final long count1;
    public final long source1;
    public final long count2;
    public final long intersection;
    public final double similarity;

    public static SimilarityResult TOMB = new SimilarityResult(-1, -1, -1, -1, -1, -1);

    public SimilarityResult(long source1, long source2, long count1, long count2, long intersection, double similarity) {
        this.source1 = source1;
        this.source2 = source2;
        this.count1 = count1;
        this.count2 = count2;
        this.intersection = intersection;
        this.similarity = similarity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimilarityResult that = (SimilarityResult) o;
        return source1 == that.source1 &&
                source2 == that.source2 &&
                count1 == that.count1 &&
                count2 == that.count2 &&
                intersection == that.intersection &&
                Double.compare(that.similarity, similarity) == 0;
    }

    @Override
    public int hashCode() {

        return Objects.hash(source1, source2, count1, count2, intersection, similarity);
    }

    public static SimilarityResult of(long source1, long source2, long[] targets1, long[] targets2, double similarityCutoff) {
        long intersection = JaccardProc.intersection3(targets1,targets2);
        if (similarityCutoff >= 0d && intersection == 0) return null;
        int count1 = targets1.length;
        int count2 = targets2.length;
        long denominator = count1 + count2 - intersection;
        double jaccard = denominator == 0 ? 0 : (double)intersection / denominator;
        if (jaccard < similarityCutoff) return null;
        return new SimilarityResult(source1, source2, count1, count2, intersection, jaccard);
    }
}
