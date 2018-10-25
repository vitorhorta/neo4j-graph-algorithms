package org.neo4j.graphalgo.ml;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

public class FeatureVectors {

    @Context
    public KernelTransaction transaction;

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
