/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl;

import org.apache.commons.lang3.time.StopWatch;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.split.FileSplit;
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.eval.ROCBinary;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.api.rng.Random;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.SplitTestAndTrain;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.dataset.api.preprocessor.DataNormalization;
import org.nd4j.linalg.dataset.api.preprocessor.NormalizerStandardize;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.indexing.INDArrayIndex;
import org.nd4j.linalg.indexing.NDArrayIndex;
import org.nd4j.linalg.learning.config.Sgd;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.nd4j.linalg.ops.transforms.Transforms;
import org.neo4j.graphalgo.BetweennessCentralityProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class DeepGLTest {

    private static GraphDatabaseAPI db;
    private static HeavyGraph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE]->(b),\n" +
                        " (a)-[:TYPE]->(f),\n" +
                        " (b)-[:TYPE]->(c),\n" +
                        " (c)-[:TYPE]->(d),\n" +
                        " (d)-[:TYPE]->(g),\n" +
                        " (d)-[:TYPE]->(e)";


        db = TestDatabaseCreator.createTestDatabase();

        try (Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        }

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(BetweennessCentralityProc.class);

        graph = (HeavyGraph) new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .load(HeavyGraphFactory.class);

    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
        graph = null;
    }

    @Test
    public void testDeepGL() throws Exception {

        DeepGL deepGL = new DeepGL(graph, Pools.DEFAULT, 3, 1, 0.8, 10, false);
        deepGL.withProgressLogger(new TestProgressLogger());
        deepGL.compute();
        Stream<DeepGL.Result> resultStream = deepGL.resultStream();

        BufferedWriter writer = new BufferedWriter(new FileWriter("out.emb"));

        resultStream
                .peek(r -> {
                    String res = r.embedding.stream()
                            .map(Object::toString)
                            .reduce((s, s2) -> String.join(" ", s, s2))
                            .get();
                    try {
                        writer.write(res);
                        writer.newLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                })
                .forEach(r -> {
//                            System.out.println("emb: " + r.embedding);
                            System.out.println("nd4j: " + r.embedding);
                        }
                );

        deepGL.featureStream().forEach(System.out::println);

        writer.close();
    }

    @Test
    public void testBooleanIndexing() {

        int rows = 3;
        int cols = 3;
        Random rnd = Nd4j.getRandomFactory().getNewRandomInstance(1234);

        INDArray randomMatrix = Nd4j.rand(rows, cols, rnd);
        System.out.println("randomMatrix = \n" + randomMatrix);

        INDArray mask = Transforms.round(Nd4j.rand(rows, cols));
        System.out.println("mask = \n" + mask);
    }

    @Test
    public void testOperatorsForNDArrays() {

        INDArray features = getFeatures();
//        INDArray adjacencyMatrix = getBOTHAdjacencyMatrix();
        INDArray adjacencyMatrix = getOUTAdjacencyMatrix();
        System.out.println("adjacencyMatrix = \n" + adjacencyMatrix);

        INDArray sum = adjacencyMatrix.mmul(features);
        System.out.println("sum = \n" + sum);

        StopWatch timer = new StopWatch();

        for (int i = 0; i < 10; i++) {
            timer.reset();
            timer.start();
            INDArray mean = adjacencyMatrix.mmul(features).div(adjacencyMatrix.sum(1).repeat(1, features.columns()));
            Nd4j.clearNans(mean);
            timer.stop();
            long meanTime = timer.getNanoTime();

            timer.reset();
            timer.start();
            INDArray mean2 = adjacencyMatrix
                    .mmul(features)
                    .diviColumnVector(adjacencyMatrix.sum(1));
            Nd4j.clearNans(mean2);
            timer.stop();
            assert mean.equals(mean2);
            System.out.printf("mean: change in time = %d ns\n", timer.getNanoTime() - meanTime);
        }

        for (int i = 0; i < 10; i++) {
            timer.reset();
            timer.start();
            INDArray[] had = new INDArray[adjacencyMatrix.columns()];
            for (int column = 0; column < adjacencyMatrix.columns(); column++) {
                int finalColumn = column;
                int[] indexes = IntStream.range(0, adjacencyMatrix.rows())
                        .filter(r -> adjacencyMatrix.getDouble(finalColumn, r) != 0)
                        .toArray();

                if (indexes.length > 0) {
                    had[column] = Nd4j.ones(features.columns());
                    for (int index : indexes) {
                        had[column].muli(features.getRow(index));
                    }
                } else {
                    INDArray zeros = Nd4j.zeros(features.columns());
                    had[column] = zeros;
                }
            }
            INDArray hadamard = Nd4j.vstack(had);
            timer.stop();
            long hadamardTime = timer.getNanoTime();
//            System.out.println("hadamard = \n" + hadamard);

            timer.reset();
            timer.start();
            INDArray addi = adjacencyMatrix.neg().addi(1);
            INDArray sums = adjacencyMatrix.sum(1);
            INDArray hadamard2 = Nd4j.zeros(7, 3);
            for (int col = 0; col < adjacencyMatrix.columns(); col++) {
                if (sums.getDouble(col) > 0) {
                    INDArray filtered = features.mulColumnVector(adjacencyMatrix.getRow(col).transpose());
                    filtered.addiColumnVector(addi.getRow(col).transpose());
                    hadamard2.getRow(col).addi(filtered.prod(0));
                }
            }
            timer.stop();
//            System.out.println("hadamard2 = \n" + hadamard2);
            System.out.printf("hadamard: change in time = %d ns\n", timer.getNanoTime() - hadamardTime);
            assert (hadamard.equals(hadamard2));
        }


        for (int i = 0; i < 10; i++) {
            timer.reset();
            timer.start();
            INDArray[] maxes = new INDArray[features.columns()];
            for (int fCol = 0; fCol < features.columns(); fCol++) {
                INDArray repeat = features.getColumn(fCol).repeat(1, adjacencyMatrix.columns());
                INDArray mul = adjacencyMatrix.transpose().mul(repeat);
                maxes[fCol] = mul.max(0).transpose();

            }
            INDArray max = Nd4j.hstack(maxes);
            timer.stop();
            long maxTime = timer.getNanoTime();
//            System.out.println("max = \n" + max);

            timer.reset();
            timer.start();
            INDArray[] maxes2 = new INDArray[features.columns()];
            for (int fCol = 0; fCol < features.columns(); fCol++) {
                INDArray mul = adjacencyMatrix.transpose().mulColumnVector(features.getColumn(fCol));
                maxes2[fCol] = mul.max(0).transpose();
            }
            INDArray max2 = Nd4j.hstack(maxes2);
            timer.stop();
//            System.out.println("max2 = \n" + max2);
            System.out.printf("max: change in time = %d ns\n", timer.getNanoTime() - maxTime);
            assert (max.equals(max2));
        }

        for (int i = 0; i < 10; i++) {
            timer.reset();
            timer.start();
            INDArray[] norms = new INDArray[adjacencyMatrix.rows()];
            for (int node = 0; node < adjacencyMatrix.rows(); node++) {
                INDArray nodeFeatures = features.getRow(node);
                INDArray adjs = adjacencyMatrix.transpose().getColumn(node).repeat(1, features.columns());
                INDArray repeat = nodeFeatures.repeat(0, features.rows()).mul(adjs);
                INDArray sub = repeat.sub(features.mul(adjs));
                INDArray norm = sub.norm1(0);
                norms[node] = norm;
            }
            INDArray l1Norm = Nd4j.vstack(norms);
            timer.stop();
            long l2NormTime = timer.getNanoTime();
            System.out.println("l1Norm = \n" + l1Norm);

            timer.reset();
            timer.start();
            INDArray[] norms2 = new INDArray[adjacencyMatrix.rows()];
            for (int node = 0; node < adjacencyMatrix.rows(); node++) {
                INDArray nodeFeatures = features.getRow(node);
                INDArray adjs = adjacencyMatrix.transpose().getColumn(node).repeat(1, features.columns());
//                INDArray repeat = nodeFeatures.repeat(0, features.rows()).mul(adjs);
                INDArray repeat = adjs.mulRowVector(nodeFeatures);
                INDArray sub = repeat.subi(features.mulColumnVector(adjacencyMatrix.getRow(node).transpose()));
                INDArray norm = sub.norm1(0);
                norms2[node] = norm;
            }
            INDArray l1Norm2 = Nd4j.vstack(norms2);
            timer.stop();
            System.out.println("l1Norm2 = \n" + l1Norm2);
            System.out.printf("l2Norm: change in time = %d ns\n", timer.getNanoTime() - l2NormTime);
            assert (l1Norm.equals(l1Norm2));
        }

        for (int i = 0; i < 10; i++) {
            timer.reset();
            timer.start();
            // original

            double sigma = 16;
            INDArray[] sumsOfSquareDiffs = new INDArray[adjacencyMatrix.rows()];
            for (int node = 0; node < adjacencyMatrix.rows(); node++) {
                INDArray nodeFeatures = features.getRow(node);
                INDArray adjs = adjacencyMatrix.getColumn(node).repeat(1, features.columns());
                INDArray repeat = nodeFeatures.repeat(0, features.rows()).mul(adjs);
                INDArray sub = repeat.sub(features.mul(adjs));
                sumsOfSquareDiffs[node] = Transforms.pow(sub, 2).sum(0);
            }
            INDArray sumOfSquareDiffs = Nd4j.vstack(sumsOfSquareDiffs).mul(-(1d / Math.pow(sigma, 2)));
            INDArray rbf = Transforms.exp(sumOfSquareDiffs);
            timer.stop();
            long l2NormTime = timer.getNanoTime();
            System.out.println("rbf = \n" + rbf);

            timer.reset();
            timer.start();
            // new
            INDArray[] sumsOfSquareDiffs2 = new INDArray[adjacencyMatrix.rows()];
            for (int node = 0; node < adjacencyMatrix.rows(); node++) {
                INDArray column = adjacencyMatrix.getColumn(node);
                INDArray repeat = features.getRow(node).repeat(0, features.rows()).muliColumnVector(column);
                INDArray sub = repeat.sub(features.mulColumnVector(column));
                sumsOfSquareDiffs2[node] = Transforms.pow(sub, 2).sum(0);
            }
            INDArray sumOfSquareDiffs2 = Nd4j.vstack(sumsOfSquareDiffs2).muli(-(1d / Math.pow(sigma, 2)));
            INDArray rbf2 = Transforms.exp(sumOfSquareDiffs2);
            timer.stop();
            System.out.println("rbf2 = \n" + rbf2);
            System.out.printf("rbf: change in time = %d ns\n", timer.getNanoTime() - l2NormTime);
            assert (rbf.equals(rbf2));
        }

    }

    @Test
    public void testOpsForNDArrays() {

        INDArray features = getFeatures();
//        INDArray adjacencyMatrix = getBOTHAdjacencyMatrix();
        INDArray adjacencyMatrix = getOUTAdjacencyMatrix();
        System.out.println("adjacencyMatrix = " + adjacencyMatrix);


        INDArray sum = adjacencyMatrix.mmul(features);
        System.out.println("sum = \n" + sum);

//        INDArray mean = adjacencyMatrix.mmul(features).div(adjacencyMatrix.sum(1).repeat(1, features.columns()));
//        Nd4j.clearNans(mean);
//        System.out.println("mean = \n" + mean);
//
//
//        INDArray[] had = new INDArray[adjacencyMatrix.columns()];
//        for (int column = 0; column < adjacencyMatrix.columns(); column++) {
//            int finalColumn = column;
//            int[] indexes = IntStream.range(0, adjacencyMatrix.rows())
//                    .filter(r -> adjacencyMatrix.getDouble(finalColumn, r) != 0)
//                    .toArray();
//
//            if (indexes.length > 0) {
//                had[column] = Nd4j.ones(features.columns());
//                for (int index : indexes) {
//                    had[column].muli(features.getRow(index));
//                }
//            } else {
//                INDArray zeros = Nd4j.zeros(features.columns());
//                had[column] = zeros;
//            }
//        }
//        INDArray hadamard = Nd4j.vstack(had);
//        System.out.println("hadamard = \n" + hadamard);
//
//        INDArray[] maxes = new INDArray[features.columns()];
//        for (int fCol = 0; fCol < features.columns(); fCol++) {
//            INDArray repeat = features.getColumn(fCol).repeat(1, adjacencyMatrix.columns());
//            INDArray mul = adjacencyMatrix.transpose().mul(repeat);
//            maxes[fCol] = mul.max(0).transpose();
//
//        }
//        INDArray max = Nd4j.hstack(maxes);
//        System.out.println("max = \n" + max);
//
//        INDArray[] norms = new INDArray[adjacencyMatrix.rows()];
//        for (int node = 0; node < adjacencyMatrix.rows(); node++) {
//            INDArray nodeFeatures = features.getRow(node);
//            INDArray adjs = adjacencyMatrix.transpose().getColumn(node).repeat(1, features.columns());
//            INDArray repeat = nodeFeatures.repeat(0, features.rows()).mul(adjs);
//            INDArray sub = repeat.sub(features.mul(adjs));
//            INDArray norm = sub.norm1(0);
//            norms[node] = norm;
//        }
//        INDArray l1Norm = Nd4j.vstack(norms);
//        System.out.println("l1Norm = \n" + l1Norm);
//
//        double sigma = 16;
//        INDArray[] sumsOfSquareDiffs = new INDArray[adjacencyMatrix.rows()];
//        for (int node = 0; node < adjacencyMatrix.rows(); node++) {
//            INDArray nodeFeatures = features.getRow(node);
//            INDArray adjs = adjacencyMatrix.getColumn(node).repeat(1, features.columns());
//            INDArray repeat = nodeFeatures.repeat(0, features.rows()).mul(adjs);
//            INDArray sub = repeat.sub(features.mul(adjs));
//            sumsOfSquareDiffs[node] = Transforms.pow(sub, 2).sum(0);
//        }
//        INDArray sumOfSquareDiffs = Nd4j.vstack(sumsOfSquareDiffs).mul(-(1d / Math.pow(sigma, 2)));
//        INDArray rbf = Transforms.exp(sumOfSquareDiffs);
//        System.out.println("rbf = " + rbf);
    }

    private INDArray getBOTHAdjacencyMatrix() {
        return Nd4j.create(new double[][]{
                {0.00, 1.00, 0.00, 0.00, 0.00, 1.00, 0.00},
                {1.00, 0.00, 1.00, 0.00, 0.00, 0.00, 0.00},
                {0.00, 1.00, 0.00, 1.00, 0.00, 0.00, 0.00},
                {0.00, 0.00, 1.00, 0.00, 1.00, 0.00, 1.00},
                {0.00, 0.00, 0.00, 1.00, 0.00, 0.00, 0.00},
                {1.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00},
                {0.00, 0.00, 0.00, 1.00, 0.00, 0.00, 0.00},
        });
    }

    private INDArray getOUTAdjacencyMatrix() {
        return Nd4j.create(new double[][]{
                    {0.00, 1.00, 0.00, 0.00, 0.00, 1.00, 0.00},
                    {0.00, 0.00, 1.00, 0.00, 0.00, 0.00, 0.00},
                    {0.00, 0.00, 0.00, 1.00, 0.00, 0.00, 0.00},
                    {0.00, 0.00, 0.00, 0.00, 1.00, 0.00, 1.00},
                    {0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00},
                    {0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00},
                    {0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00},

            });
    }

    private INDArray getFeatures() {
        INDArray features = Nd4j.create(new double[][]{
                {0.00, 1.00, 0.00},
                {0.00, 0.00, 1.00},
                {0.00, 1.00, 1.00},
                {0.00, 2.00, 2.00},
                {1.00, 0.00, 0.00},
                {1.00, 0.00, 0.00},
                {2.00, 0.00, 0.00},
        });
        System.out.println("features = \n" + features);
        return features;
    }

    @Test
    public void runClassifierOverEmbeddingFiles() throws IOException {
        Process exec = Runtime.getRuntime().exec("ls\n");
        InputStream inputStream = exec.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        reader.lines()
                .flatMap(s -> Arrays.stream(s.split("\\s")))
                .filter(s -> s.startsWith("embedding"))
                .filter(s -> s.endsWith(".txt"))
                .peek(System.out::println)
                .forEach(inFileName -> {
                    try {
                        String results = classifyAndEvaluate(inFileName);
                        String fileName = inFileName.substring(0, inFileName.length() - 4);
                        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(fileName + ".result")));
                        writer.write(fileName + "\n");
                        writer.write(results + "\n");
                        writer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }


    @Test
    public void testingViews() {
        INDArray reshape = Nd4j.arange(20).reshape(4, 5);
        System.out.println("reshape = \n" + reshape);
        INDArrayIndex[] interval = NDArrayIndex.indexesFor(2);
        INDArray indArray = reshape.get(interval);
        System.out.println("indArray = \n" + indArray);
    }

    public String classifyAndEvaluate(String file) {

        //First: get the dataset using the record reader. CSVRecordReader handles loading/parsing
        int numLinesToSkip = 0;
        char delimiter = ' ';
        RecordReader recordReader = new CSVRecordReader(numLinesToSkip,delimiter);
        File dataFile = new File(file);

        BufferedReader reader = null;
        try {
            recordReader.initialize(new FileSplit(dataFile));
            reader = new BufferedReader(new FileReader(dataFile));
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        assert reader != null;
        int numCols = reader.lines()
                .findFirst()
                .map(l -> l.split((String.valueOf(delimiter))))
                .map(arr -> arr.length)
                .get();

        int numExamples = (int) reader.lines().count();

        //Second: the RecordReaderDataSetIterator handles conversion to DataSet objects, ready for use in neural network
        int numInputs = numCols - 1;
        int labelIndex = numInputs;
        int numClasses = 6;
        int batchSize = numExamples;
        long seed = 6;

        DataSetIterator iterator = new RecordReaderDataSetIterator(recordReader,batchSize,labelIndex,numClasses);
        DataSet allData = iterator.next();
        allData.shuffle(seed);
        SplitTestAndTrain testAndTrain = allData.splitTestAndTrain(0.67);  //Use 65% of data for training

        DataSet trainingData = testAndTrain.getTrain();
        DataSet testData = testAndTrain.getTest();

        //We need to normalize our data. We'll use NormalizeStandardize (which gives us mean 0, unit variance):
        DataNormalization normalizer = new NormalizerStandardize();
        normalizer.fit(trainingData);           //Collect the statistics (mean/stdev) from the training data. This does not modify the input data
        normalizer.transform(trainingData);     //Apply normalization to the training data
        normalizer.transform(testData);         //Apply normalization to the test data. This is using statistics calculated from the *training* set



        System.out.println("Build model....");
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(seed)
                .activation(Activation.TANH)
                .weightInit(WeightInit.XAVIER)
                .updater(new Sgd(0.001))
                .l2(1e-4)
                .list()
                .layer(0, new DenseLayer.Builder().nIn(numInputs).nOut(30)
                        .build())
                .layer(1, new DenseLayer.Builder().nIn(30).nOut(30)
                        .build())
                .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.MSE)
                        .activation(Activation.SOFTMAX)
                        .nIn(30).nOut(numClasses).build())
                .backprop(true).pretrain(false)
                .build();

        //run the model
        MultiLayerNetwork model = new MultiLayerNetwork(conf);
        model.init();
        model.setListeners(new ScoreIterationListener(100));

        for(int i=0; i<10000; i++ ) {
            model.fit(trainingData);
        }

        //evaluate the model on the test set
        Evaluation eval = new Evaluation(numClasses);
        INDArray output = model.output(testData.getFeatureMatrix());
        eval.eval(testData.getLabels(), output);
        System.out.println(eval.stats());

        // accuracy by class
        Map<Integer, Integer> truePositivesMap = eval.truePositives();
        for (Integer clazz : truePositivesMap.keySet()) {
            Integer truePositives = truePositivesMap.get(clazz);
            int total = eval.classCount(clazz);
            System.out.printf("Accuracy for class %d: %f\n", clazz, (double) truePositives / total);
        }

        ROCBinary rocEval = new ROCBinary(0);
        rocEval.eval(testData.getLabels(), output);
        System.out.println(rocEval.stats());
        System.out.println("Average AUC: " + rocEval.calculateAverageAuc());
        System.out.println("========================================================================\n\n");
        return eval.stats()
                + rocEval.stats()
                + "\nAverage AUC: " + rocEval.calculateAverageAuc()
                + "\n========================================================================\n\n";


    }
}
