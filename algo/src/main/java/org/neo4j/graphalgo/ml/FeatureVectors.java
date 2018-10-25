package org.neo4j.graphalgo.ml;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class FeatureVectors {

    @UserFunction("algo.ml.oneHotEncoding")
    @Description("CALL algo.ml.oneHotEncoding(availableValues, selectedValues) - return a list of selected values in a one hot encoding format.")
    public List<Long> oneHotEncoding(@Name(value = "availableValues") List<Object> availableValues,
                                     @Name(value = "selectedValues") List<Object> selectedValues) {
        if (availableValues == null) {
            return LongStream.empty().boxed().collect(Collectors.toList());
        }

        if (selectedValues == null) {
            return LongStream.range(0, availableValues.size()).map(index -> 0).boxed().collect(Collectors.toList());
        }

        Set<Object> selectedValuesSet = new HashSet<>(selectedValues);
        Object[] availableValuesArray = availableValues.toArray();
        return LongStream.range(0, availableValues.size())
                .map(index -> selectedValuesSet.contains(availableValuesArray[(int) index]) ? 1L : 0L)
                .boxed()
                .collect(Collectors.toList());
    }

    @UserFunction("algo.ml.featureVector")
    @Description("CALL algo.ml.featureVector(availableValues, weights) - return a list of selected weights as a feature vector.")
    public List<Double> featureVector(@Name(value = "availableValues") List<Object> availableValues,
                                      @Name(value = "weights") List<Map<String, Object>> weights) {
        if (availableValues == null) {
            return DoubleStream.empty().boxed().collect(Collectors.toList());
        }

        if (weights == null || weights.size() == 0) {
            return LongStream.range(0, availableValues.size()).mapToDouble(index -> 0D).boxed().collect(Collectors.toList());
        }

        Object[] availableValuesArray = availableValues.toArray();
        Map<Object, Double> weightsMap = weights
                .stream()
                .collect(Collectors.toMap(s -> s.get("item"), this::extractWeight, (x1, x2) -> x1));

        return LongStream.range(0, availableValues.size())
                .mapToDouble(index -> weightsMap.getOrDefault(availableValuesArray[(int) index], 0.0D)).boxed()
                .collect(Collectors.toList());
    }

    @Procedure("algo.ml.featureVector")
    public Stream<FeatureVectorResult> featureVectorProc(@Name(value = "availableValues") List<Object> availableValues,
                                                         @Name(value = "data", defaultValue = "null") List<Map<String, Object>> data) {
        Object[] availableValuesArray = availableValues.toArray();

        FeatureVectorResult[] featureVectorResults = new FeatureVectorResult[data.size()];
        int idx = 0;
        for (Map<String, Object> row : data) {
            Map<Object, Double> weightsMap = ((List<Map<String, Object>>) row.get("selectedValues")).stream()
                    .collect(Collectors.toMap(s -> s.get("item"), this::extractWeight, (x1, x2) -> x1));

            List<Double> featureVector = LongStream.range(0, availableValues.size())
                    .mapToDouble(index -> weightsMap.getOrDefault(availableValuesArray[(int) index], 0.0D)).boxed()
                    .collect(Collectors.toList());

            featureVectorResults[idx++] = new FeatureVectorResult((long) row.get("item"), featureVector);
        }
        return Stream.of(featureVectorResults);
    }

//    WeightedInput[] prepareWeights(List<Map<String, Object>> data, long degreeCutoff) {
//        WeightedInput[] inputs = new WeightedInput[data.size()];
//        int idx = 0;
//        for (Map<String, Object> row : data) {
//
//            List<Number> weightList = extractValues(row.get("weights"));
//
//            int size = weightList.size();
//            if (size > degreeCutoff) {
//                double[] weights = new double[size];
//                int i = 0;
//                for (Number value : weightList) {
//                    weights[i++] = value.doubleValue();
//                }
//                inputs[idx++] = new WeightedInput((Long) row.get("item"), weights);
//            }
//        }
//        if (idx != inputs.length) inputs = Arrays.copyOf(inputs, idx);
//        Arrays.sort(inputs);
//        return inputs;
//    }
//
//    static class WeightedInput {
//        long id;
//        double[] weights;
//        int count;
//    }

    public static class FeatureVectorResult {
        public long nodeId;
        public List<Double> featureVector;

        public FeatureVectorResult(long nodeid, List<Double> featureVector) {
            this.nodeId = nodeid;
            this.featureVector = featureVector;
        }
    }

    private double extractWeight(Map<String, Object> map) {
        Object weight = map.get("weight");
        if (weight instanceof Integer) {
            return ((Integer) weight).doubleValue();
        }
        if (weight instanceof Long) {
            return ((Long) weight).doubleValue();
        }

        return (Double) weight;
    }
}
