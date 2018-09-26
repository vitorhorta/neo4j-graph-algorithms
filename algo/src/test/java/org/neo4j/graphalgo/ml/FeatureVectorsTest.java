package org.neo4j.graphalgo.ml;

import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;

import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class FeatureVectorsTest {

    @Test
    public void singleCategorySelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        Map<String, Double> weights = new HashMap<>();
        weights.put("Italian", 3D);
        weights.put("Indian", 5D);

        assertEquals(asList(3D, 5D, 0D), new FeatureVectors().featureVector(values, weights));
    }

    @Test
    public void noCategoriesSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        Map<String, Double> weights = new HashMap<>();

        assertEquals(asList(0D, 0D, 0D), new FeatureVectors().featureVector(values, weights));
    }

    @Test
    public void moreThanOneSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Arrays.asList("Italian", "Chinese");

        assertEquals(asList(1L, 0L, 1L), new FeatureVectors().oneHotEncoding(values, selectedValues));
    }

    @Test
    public void allSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Arrays.asList("Italian", "Chinese", "Indian");

        assertEquals(asList(1L, 1L, 1L), new FeatureVectors().oneHotEncoding(values, selectedValues));
    }

    @Test
    public void nonExistentSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Collections.singletonList("British");

        assertEquals(asList(0L, 0L, 0L), new FeatureVectors().oneHotEncoding(values, selectedValues));
    }

    @Test
    public void oneNonExistentSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Object> selectedValues = Arrays.asList("British", "Chinese");

        assertEquals(asList(0L, 0L, 1L), new FeatureVectors().oneHotEncoding(values, selectedValues));
    }

    @Test
    public void nullSelectedMeansNoneSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");

        assertEquals(asList(0D, 0D, 0D), new FeatureVectors().featureVector(values, null));
    }

    @Test
    public void nullAvailableMeansEmptyArray() {
        assertEquals(Collections.emptyList(), new FeatureVectors().featureVector(null, null));
    }

}