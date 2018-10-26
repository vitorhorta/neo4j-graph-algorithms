package org.neo4j.graphalgo;

import com.jujutsu.tsne.TSneConfiguration;
import com.jujutsu.tsne.barneshut.BHTSne;
import com.jujutsu.tsne.barneshut.BarnesHutTSne;
import com.jujutsu.tsne.barneshut.ParallelBHTsne;
import com.jujutsu.utils.MatrixUtils;
import com.jujutsu.utils.TSneUtils;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.similarity.WeightedInput;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.similarity.SimilarityProc.prepareWeights;

public class TSNEProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.ml.tsne.stream", mode = Mode.READ)
    @Description("CALL algo.pageRank.stream(label:String, relationship:String, " +
            "{iterations:20, dampingFactor:0.85, weightProperty: null, concurrency:4}) " +
            "YIELD node, score - calculates page rank and streams results")
    public Stream<TSNEResult> tsneStream(
            @Name(value = "data", defaultValue = "null") List<Map<String,Object>> data,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        WeightedInput[] inputs = prepareWeights(data, 0);

        double perplexity = configuration.get("perplexity", 20.0);
        boolean parallel = configuration.get("parallel", false);
        Long maxIterations = configuration.get("maxIterations", 1000L);
        Long outputDimensions = configuration.get("outputDimensions", 2L);
        boolean logProgress = configuration.get("logProgress", false);
        double theta = configuration.get("theta", 0.5);
        boolean usePCA = configuration.get("usePCA", true);


        int initialDimensions = inputs[0].dimension();

        double [][] X = new double[inputs.length][];
        for (int i = 0; i < inputs.length; i++) {
            X[i] = inputs[i].weights();
        }

        BarnesHutTSne tsne;
        if(parallel) {
            tsne = new ParallelBHTsne();
        } else {
            tsne = new BHTSne();
        }

        TSneConfiguration tSneConfiguration = TSneUtils.buildConfig(
                X, outputDimensions.intValue(), initialDimensions, perplexity, maxIterations.intValue(), usePCA, theta, !logProgress
        );
        double [][] Y = tsne.tsne(tSneConfiguration);

        return IntStream.range(0, Y.length).mapToObj(index -> new TSNEResult(inputs[index].id(), Y[index]));
    }

    public static class TSNEResult {
        public final long nodeId;
        public final List<Double> value;

        public TSNEResult(long nodeId, double[] value) {

            this.nodeId = nodeId;

            this.value = new ArrayList<>(value.length);
            for (double item : value) {
                this.value.add(item);
            }
        }
    }
}
