package org.neo4j.graphalgo.core.utils;

import org.junit.Test;
import org.neo4j.graphalgo.similarity.Weights;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class IntersectionsTest {
    @Test
    public void rleCosineVector1Repeats() throws Exception {
        List<Number> vector1List = Arrays.asList(5.0, 5.0, 5.0, 5.0, 5.0);
        List<Number> vector2List = Arrays.asList(2.0, 3.0, 4.0, 5.0, 6.0);

        double[] vector1 = Weights.buildWeights(vector1List);
        double[] vector2 = Weights.buildWeights(vector2List);
        int len = vector1List.size();

        double similarity = Intersections.cosineSquareSkip(vector1, vector2, len, Double.NaN);
        System.out.println("v = " + similarity);


        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);
        double[] vector2Rle = Weights.buildRleWeights(vector2List ,3);

        System.out.println("vector1Rle = " + Arrays.toString(vector1Rle));
        System.out.println("vector2Rle = " + Arrays.toString(vector2Rle));

        double similarityRle = Intersections.cosineSquareRleSkip(vector1Rle, vector2Rle, len, Double.NaN);

        assertEquals(similarity, similarityRle, 0.01);
    }

    @Test
    public void rleCosineVector1MultipleRepeats() throws Exception {
        List<Number> vector1List = Arrays.asList(5.0, 5.0, 5.0, 5.0, 5.0, 4.0, 4.0, 4.0, 4.0);
        List<Number> vector2List = Arrays.asList(2.0, 3.0, 4.0, 5.0, 6.0, 1.0, 2.0, 3.0, 4.0);

        double[] vector1 = Weights.buildWeights(vector1List);
        double[] vector2 = Weights.buildWeights(vector2List);
        int len = vector1List.size();

        double similarity = Intersections.cosineSquareSkip(vector1, vector2, len, Double.NaN);
        System.out.println("v = " + similarity);


        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);
        double[] vector2Rle = Weights.buildRleWeights(vector2List ,3);

        System.out.println("vector1Rle = " + Arrays.toString(vector1Rle));
        System.out.println("vector2Rle = " + Arrays.toString(vector2Rle));

        double similarityRle = Intersections.cosineSquareRleSkip(vector1Rle, vector2Rle, len, Double.NaN);

        assertEquals(similarity, similarityRle, 0.01);
    }

    @Test
    public void rleCosineVector1Mixed() throws Exception {
        List<Number> vector1List = Arrays.asList(5.0, 5.0, 5.0, 5.0, 5.0, 3.0, 2.0, 4.0, 4.0, 4.0, 4.0);
        List<Number> vector2List = Arrays.asList(2.0, 3.0, 4.0, 5.0, 6.0, 4.0, 3.0, 1.0, 2.0, 3.0, 4.0);

        double[] vector1 = Weights.buildWeights(vector1List);
        double[] vector2 = Weights.buildWeights(vector2List);
        int len = vector1List.size();

        double similarity = Intersections.cosineSquareSkip(vector1, vector2, len, Double.NaN);
        System.out.println("v = " + similarity);


        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);
        double[] vector2Rle = Weights.buildRleWeights(vector2List ,3);

        System.out.println("vector1Rle = " + Arrays.toString(vector1Rle));
        System.out.println("vector2Rle = " + Arrays.toString(vector2Rle));

        double similarityRle = Intersections.cosineSquareRleSkip(vector1Rle, vector2Rle, len, Double.NaN);

        assertEquals(similarity, similarityRle, 0.01);
    }

    @Test
    public void rleCosineNoRepeats() throws Exception {
        List<Number> vector1List = Arrays.asList(5.0, 4.0, 5.0, 4.0, 5.0);
        List<Number> vector2List = Arrays.asList(2.0, 3.0, 4.0, 5.0, 6.0);

        double[] vector1 = Weights.buildWeights(vector1List);
        double[] vector2 = Weights.buildWeights(vector2List);
        int len = vector1List.size();

        double similarity = Intersections.cosineSquareSkip(vector1, vector2, len, Double.NaN);
        System.out.println("v = " + similarity);


        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);
        double[] vector2Rle = Weights.buildRleWeights(vector2List ,3);

        System.out.println("vector1Rle = " + Arrays.toString(vector1Rle));
        System.out.println("vector2Rle = " + Arrays.toString(vector2Rle));

        double similarityRle = Intersections.cosineSquareRleSkip(vector1Rle, vector2Rle, len, Double.NaN);

        assertEquals(similarity, similarityRle, 0.01);
    }


//    @Test
//    public void rleCosineVector2Repeats() throws Exception {
//        List<Number> vector1List = Arrays.asList(2.0, 3.0, 4.0, 5.0, 6.0);
//        List<Number> vector2List = Arrays.asList(5.0, 5.0, 5.0, 5.0, 5.0);
//
//        double[] vector1 = Weights.buildWeights(vector1List);
//        double[] vector2 = Weights.buildWeights(vector2List);
//        int len = vector1List.size();
//
//        double similarity = Intersections.cosineSquareSkip(vector1, vector2, len, Double.NaN);
//        System.out.println("v = " + similarity);
//
//
//        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);
//        double[] vector2Rle = Weights.buildRleWeights(vector2List ,3);
//
//        System.out.println("vector1Rle = " + Arrays.toString(vector1Rle));
//        System.out.println("vector2Rle = " + Arrays.toString(vector2Rle));
//
//        double similarityRle = Intersections.cosineSquareRleSkip(vector1Rle, vector2Rle, len, Double.NaN);
//
//        assertEquals(similarity, similarityRle, 0.01);
//    }

    @Test
    public void rleCosineBothRepeats() throws Exception {
        List<Number> vector1List = Arrays.asList(5.0, 5.0, 5.0, 5.0, 5.0);
        List<Number> vector2List = Arrays.asList(5.0, 5.0, 5.0, 5.0, 5.0);

        double[] vector1 = Weights.buildWeights(vector1List);
        double[] vector2 = Weights.buildWeights(vector2List);
        int len = vector1List.size();

        double similarity = Intersections.cosineSquareSkip(vector1, vector2, len, Double.NaN);
        System.out.println("v = " + similarity);


        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);
        double[] vector2Rle = Weights.buildRleWeights(vector2List ,3);

        System.out.println("vector1Rle = " + Arrays.toString(vector1Rle));
        System.out.println("vector2Rle = " + Arrays.toString(vector2Rle));

        double similarityRle = Intersections.cosineSquareRleSkip(vector1Rle, vector2Rle, len, Double.NaN);

        assertEquals(similarity, similarityRle, 0.01);
    }

//    @Test
//    public void rleCosineBothRepeatsOfDifferentSizes() throws Exception {
//        List<Number> vector1List = Arrays.asList(5.0, 5.0, 5.0, 5.0, 5.0, 4.0);
//        List<Number> vector2List = Arrays.asList(5.0, 5.0, 5.0, 5.0, 5.0, 5.0);
//
//        double[] vector1 = Weights.buildWeights(vector1List);
//        double[] vector2 = Weights.buildWeights(vector2List);
//        System.out.println("vector1 = " + Arrays.toString(vector1));
//        System.out.println("vector2 = " + Arrays.toString(vector2));
//        int len = vector1List.size();
//
//        double similarity = Intersections.cosineSquareSkip(vector1, vector2, len, Double.NaN);
//        System.out.println("v = " + similarity);
//
//
//        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);
//        double[] vector2Rle = Weights.buildRleWeights(vector2List ,3);
//
//        System.out.println("vector1Rle = " + Arrays.toString(vector1Rle));
//        System.out.println("vector2Rle = " + Arrays.toString(vector2Rle));
//
//        double similarityRle = Intersections.cosineSquareRleSkip(vector1Rle, vector2Rle, len, Double.NaN);
//
//        assertEquals(similarity, similarityRle, 0.01);
//    }


}
