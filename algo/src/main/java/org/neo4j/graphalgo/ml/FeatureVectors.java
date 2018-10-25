package org.neo4j.graphalgo.ml;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.QueueBasedSpliterator;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.*;

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

    @Procedure("algo.ml.featureVector.stream")
    public Stream<FeatureVectorResult> featureVectorProc(@Name(value = "availableValues") List<Object> availableValues,
                                                         @Name(value = "data", defaultValue = "null") List<Map<String, Object>> data,
                                                         @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (availableValues == null || data == null) {
            return Stream.empty();
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        Object[] availableValuesArray = availableValues.toArray();

        int concurrency = configuration.getConcurrency();
        if(concurrency == 1) {
            return sequentialFeatureVectors(data, availableValuesArray);
        } else {
            return parallelFeatureVectors(data, availableValuesArray, concurrency);
        }
    }

    private Stream<FeatureVectorResult> sequentialFeatureVectors(@Name(value = "data", defaultValue = "null") List<Map<String, Object>> data, Object[] availableValuesArray) {
        FeatureVectorResult[] featureVectorResults = new FeatureVectorResult[data.size()];
        int idx = 0;
        for (Map<String, Object> row : data) {
            Map<Object, Double> weightsMap = ((List<Map<String, Object>>) row.get("selectedValues")).stream()
                    .collect(Collectors.toMap(s -> s.get("item"), this::extractWeight, (x1, x2) -> x1));

            List<Double> featureVector = LongStream.range(0, availableValuesArray.length)
                    .mapToDouble(index -> weightsMap.getOrDefault(availableValuesArray[(int) index], 0.0D)).boxed()
                    .collect(Collectors.toList());

            featureVectorResults[idx++] = new FeatureVectorResult((long) row.get("item"), featureVector);
        }
        return Stream.of(featureVectorResults);
    }

    private Stream<FeatureVectorResult> parallelFeatureVectors(@Name(value = "data", defaultValue = "null") List<Map<String, Object>> data, Object[] availableValuesArray, int concurrency) {
        int timeout = 100;
        int queueSize = 1000;
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        int batchSize = ParallelUtil.adjustBatchSize(data.size(), concurrency, 100);
        Collection<Runnable> tasks = new ArrayList<>((data.size() / batchSize) + 1);

        ArrayBlockingQueue<FeatureVectorResult> queue = new ArrayBlockingQueue<>(queueSize);

        Iterator<Map<String, Object>> dataIterator = data.iterator();
        while (dataIterator.hasNext()) {

            List<Map<String, Object>> ids = new ArrayList<>(batchSize);
            int i=0;
            while (i<batchSize && dataIterator.hasNext()) {
                ids.add(i++, dataIterator.next());
            }
            int size = i;
            tasks.add(() -> {
                for (int j = 0; j < size; j++) {
                    put(queue,createFeatureVector(availableValuesArray, ids.get(j)));
                }
            });
        }
        new Thread(() -> {
            ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);
            put(queue, FeatureVectorResult.TOMB);
        }).start();

        QueueBasedSpliterator<FeatureVectorResult> spliterator = new QueueBasedSpliterator<>(queue, FeatureVectorResult.TOMB, terminationFlag, timeout);
        return StreamSupport.stream(spliterator, false);
    }

    private FeatureVectorResult createFeatureVector(Object[] availableValuesArray, Map<String, Object> row) {
        Map<Object, Double> weightsMap = ((List<Map<String, Object>>) row.get("selectedValues")).stream()
                .collect(Collectors.toMap(s -> s.get("item"), this::extractWeight, (x1, x2) -> x1));

        List<Double> featureVector = LongStream.range(0, availableValuesArray.length)
                .mapToDouble(index -> weightsMap.getOrDefault(availableValuesArray[(int) index], 0.0D)).boxed()
                .collect(Collectors.toList());

        return new FeatureVectorResult((long) row.get("item"), featureVector);
    }

    private static <T> void put(BlockingQueue<T> queue, T items) {
        try {
            queue.put(items);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public static class FeatureVectorResult {
        public static FeatureVectorResult TOMB = new FeatureVectorResult(-1, Collections.emptyList());

        public long nodeId;
        public List<Double> featureVector;

        public FeatureVectorResult(long nodeid, List<Double> featureVector) {
            this.nodeId = nodeid;
            this.featureVector = featureVector;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FeatureVectorResult that = (FeatureVectorResult) o;
            return nodeId == that.nodeId &&
                    Objects.equals(featureVector, that.featureVector);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, featureVector);
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
