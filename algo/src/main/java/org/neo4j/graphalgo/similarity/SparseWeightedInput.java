package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.utils.Intersections;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class SparseWeightedInput extends WeightedInput {
    private final Map<Long, Double> weights;
    int count;

    public SparseWeightedInput(long id, Map<Long, Double> weights) {
        super(id);
        this.weights = weights;
        this.count = weights.size();
    }

    SimilarityResult sumSquareDelta(double similarityCutoff, SparseWeightedInput other) {
        List<Double> thisAcceptedWeights = new ArrayList<>();
        List<Double> otherAcceptedWeights = new ArrayList<>();
        for (Map.Entry<Long, Double> row : this.weights.entrySet()) {
            if (other.weights.containsKey(row.getKey())) {
                thisAcceptedWeights.add(row.getValue());
                otherAcceptedWeights.add(other.weights.get(row.getKey()));
            }
        }

        int len = thisAcceptedWeights.size();
        double[] weights = toArray(thisAcceptedWeights, len);
        double[] otherWeights = toArray(otherAcceptedWeights, len);

        double sumSquareDelta = Intersections.sumSquareDelta(weights, otherWeights, len);
        long intersection = 0;
        /* todo
        for (int i = 0; i < len; i++) {
            if (weights[i] == other.weights[i] && weights[i] != 0d) intersection++;
        }
        */
        if (similarityCutoff >= 0d && sumSquareDelta > similarityCutoff) return null;
        return new SimilarityResult(id, other.id, count, other.count, intersection, sumSquareDelta);
    }

    SimilarityResult cosineSquares(double similarityCutoff, SparseWeightedInput other) {
        List<Double> thisAcceptedWeights = new ArrayList<>();
        List<Double> otherAcceptedWeights = new ArrayList<>();
        for (Map.Entry<Long, Double> row : this.weights.entrySet()) {
            if (other.weights.containsKey(row.getKey())) {
                thisAcceptedWeights.add(row.getValue());
                otherAcceptedWeights.add(other.weights.get(row.getKey()));
            }
        }

        int len = thisAcceptedWeights.size();
        double[] weights = toArray(thisAcceptedWeights, len);
        double[] otherWeights = toArray(otherAcceptedWeights, len);

        double cosineSquares = Intersections.cosineSquare(weights, otherWeights, len);
        long intersection = 0;
        /* todo
        for (int i = 0; i < len; i++) {
            if (weights[i] == other.weights[i] && weights[i] != 0d) intersection++;
        }
        */
        if (similarityCutoff >= 0d && (cosineSquares == 0 || cosineSquares < similarityCutoff)) return null;
        return new SimilarityResult(id, other.id, count, other.count, intersection, cosineSquares);
    }

    private double[] toArray(List<Double> weightsList, int len) {
        double[] weights = new double[len];
        for (int i = 0; i < weightsList.size(); i++) {
            weights[i] = weightsList.get(i);
        }
        return weights;
    }
}
