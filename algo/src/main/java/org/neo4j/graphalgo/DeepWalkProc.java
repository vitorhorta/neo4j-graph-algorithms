package org.neo4j.graphalgo;

import org.deeplearning4j.graph.api.NoEdgeHandling;
import org.deeplearning4j.graph.api.Vertex;
import org.deeplearning4j.graph.models.deepwalk.DeepWalk;
import org.jetbrains.annotations.NotNull;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.walking.DeepWalkResult;
import org.neo4j.graphalgo.results.PageRankScore;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.neo4j.values.storable.INDArrayPropertyTranslator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class DeepWalkProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;


    @Procedure(value = "algo.deepWalk", mode = Mode.WRITE)
    @Description("CALL algo.deepWalk(label:String, relationship:String, " +
            "{graph: 'heavy/cypher', vectorSize:10, windowSize:2, learningRate:0.01 concurrency:4, direction:'BOTH}) " +
            "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty" +
            " - calculates page rank and potentially writes back")
    public Stream<PageRankScore.Stats> deepWalk(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        AllocationTracker tracker = AllocationTracker.create();

        PageRankScore.Stats.Builder builder = new PageRankScore.Stats.Builder();
        final Graph graph = load(label, relationship, tracker, configuration.getGraphImpl(), builder, configuration);

        int nodeCount = Math.toIntExact(graph.nodeCount());
        if (nodeCount == 0) {
            graph.release();
            return Stream.empty();
        }

        org.deeplearning4j.graph.graph.Graph<Integer, Integer> iGraph = buildDl4jGraph(graph);
        DeepWalk<Integer, Integer> dw = runDeepWalk(iGraph, configuration);

        if (configuration.isWriteFlag()) {
            final String writeProperty = configuration.getWriteProperty("deepWalk");
            builder.timeWrite(() -> Exporter.of(api, graph)
                    .withLog(log)
                    .parallel(Pools.DEFAULT, configuration.getConcurrency(), TerminationFlag.wrap(transaction))
                    .build()
                    .write(
                            writeProperty,
                            dw,
                            new INDArrayPropertyTranslator()
                    )
            );
        }

        return Stream.of(builder.build());
    }


    @Procedure(name = "algo.deepWalk.stream", mode = Mode.READ)
    @Description("CALL algo.deepWalk.stream(label:String, relationship:String, {graph: 'heavy/cypher', walkLength:10, vectorSize:10, windowSize:2, learningRate:0.01 concurrency:4, direction:'BOTH'}) " +
            "YIELD nodeId, embedding - compute embeddings for each node")
    public Stream<DeepWalkResult> deepWalkStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        AllocationTracker tracker = AllocationTracker.create();

        final Graph graph = load(label, relationship, tracker, configuration.getGraphImpl(), new PageRankScore.Stats.Builder(), configuration);

        int nodeCount = Math.toIntExact(graph.nodeCount());
        if (nodeCount == 0) {
            graph.release();
            return Stream.empty();
        }

        org.deeplearning4j.graph.graph.Graph<Integer, Integer> iGraph = buildDl4jGraph(graph);
        DeepWalk<Integer, Integer> dw = runDeepWalk(iGraph, configuration);

        return IntStream.range(0, dw.numVertices()).mapToObj(index ->
                new DeepWalkResult(graph.toOriginalNodeId(index), dw.getVertexVector(index).toDoubleVector()));
    }

    @NotNull
    private org.deeplearning4j.graph.graph.Graph<Integer, Integer> buildDl4jGraph(Graph graph) {
        List<Vertex<Integer>> nodes = new ArrayList<>();

        PrimitiveIntIterator nodeIterator = graph.nodeIterator();
        while(nodeIterator.hasNext()) {
            int nodeId = nodeIterator.next();
            nodes.add(new Vertex<>(nodeId,nodeId));
        }

        org.deeplearning4j.graph.graph.Graph<Integer, Integer> iGraph = new org.deeplearning4j.graph.graph.Graph<>(nodes);

        nodeIterator = graph.nodeIterator();
        while(nodeIterator.hasNext()) {
            int nodeId = nodeIterator.next();
            graph.forEachRelationship(nodeId, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
                iGraph.addEdge(nodeId, targetNodeId, -1, false);
                return false;
            });
        }
        return iGraph;
    }

    private DeepWalk<Integer, Integer> runDeepWalk(org.deeplearning4j.graph.graph.Graph<Integer, Integer> iGraph, ProcedureConfiguration configuration) {
        long vectorSize = configuration.get("vectorSize", 10L);
        double learningRate = configuration.get("learningRate", 0.01);
        long  windowSize = configuration.get("windowSize", 2L);
        long walkLength = configuration.get("walkSize", 10L);
        long numberOfWalks = configuration.get("numberOfWalks", 10L);

        Map<String, Number> params = new HashMap<>();
        params.put("vectorSize", vectorSize);
        params.put("learningRate", learningRate);
        params.put("windowSize", windowSize);
        params.put("walkLength", walkLength);
        params.put("numberOfWalks", numberOfWalks);

        log.info("Executing DeepWalk with params: %s", params);


        DeepWalk.Builder<Integer, Integer> builder = new DeepWalk.Builder<>();
        builder.vectorSize((int) vectorSize);
        builder.learningRate(learningRate);
        builder.windowSize((int) windowSize);
        DeepWalk<Integer, Integer> dw = builder.build();

        dw.initialize(iGraph);

        dw.fit(new MyRandomWalkGraphIteratorProvider<>(
                iGraph, (int) walkLength, 1,
                NoEdgeHandling.SELF_LOOP_ON_DISCONNECTED, (int) numberOfWalks));
        return dw;
    }


    private Graph load(
            String label,
            String relationship,
            AllocationTracker tracker,
            Class<? extends GraphFactory> graphFactory,
            PageRankScore.Stats.Builder statsBuilder, ProcedureConfiguration configuration) {

        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withAllocationTracker(tracker)
                .withDirection(configuration.getDirection(Direction.BOTH))
                .withoutNodeProperties()
                .withoutNodeWeights()
                .withoutRelationshipWeights();


        try (ProgressTimer timer = ProgressTimer.start()) {
            Graph graph = graphLoader.load(graphFactory);
            statsBuilder.withNodes(graph.nodeCount());
            return graph;
        }
    }

}
