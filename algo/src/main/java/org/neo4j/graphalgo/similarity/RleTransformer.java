package org.neo4j.graphalgo.similarity;

public class RleTransformer {
    public static double[] decode(double[] rleVector, int initialSize) {
        RleReader reader = new RleReader(rleVector);
        double[] fullVector = new double[initialSize];

        for (int i = 0; i < fullVector.length; i++) {
            reader.next();
            fullVector[i] = reader.value();
        }
        return fullVector;
    }
}
