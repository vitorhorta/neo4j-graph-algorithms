package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.utils.Intersections;

class DenseWeightedInput extends WeightedInput {
    double[] weights;
    int count;

    public DenseWeightedInput(long id, double[] weights) {
        super(id);
        this.weights = weights;
        for (double weight : weights) {
            if (weight!=0d) this.count++;
        }
    }

    SimilarityResult sumSquareDelta(double similarityCutoff, DenseWeightedInput other) {
        int len = Math.min(weights.length, other.weights.length);
        double sumSquareDelta = Intersections.sumSquareDelta(weights, other.weights, len);
        long intersection = 0;
        /* todo
        for (int i = 0; i < len; i++) {
            if (weights[i] == other.weights[i] && weights[i] != 0d) intersection++;
        }
        */
        if (similarityCutoff >= 0d && sumSquareDelta > similarityCutoff) return null;
        return new SimilarityResult(id, other.id, count, other.count, intersection, sumSquareDelta);
    }
    SimilarityResult cosineSquares(double similarityCutoff, DenseWeightedInput other) {
        int len = Math.min(weights.length, other.weights.length);
        double cosineSquares = Intersections.cosineSquare(weights, other.weights, len);
        long intersection = 0;
        /* todo
        for (int i = 0; i < len; i++) {
            if (weights[i] == other.weights[i] && weights[i] != 0d) intersection++;
        }
        */
        if (similarityCutoff >= 0d && (cosineSquares == 0 || cosineSquares < similarityCutoff)) return null;
        return new SimilarityResult(id, other.id, count, other.count, intersection, cosineSquares);
    }
}
