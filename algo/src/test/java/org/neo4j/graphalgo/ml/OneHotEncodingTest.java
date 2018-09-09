package org.neo4j.graphalgo.ml;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class OneHotEncodingTest {

    @Test
    public void singleCategorySelected() {
        List<Object> categories = asList("Italian", "Indian", "Chinese");
        List<Object> selectedCategories = Collections.singletonList("Italian");

        assertEquals(asList(1L, 0L, 0L), new OneHotEncoding().oneHotEncoding(categories, selectedCategories));
    }

    @Test
    public void noCategoriesSelected() {
        List<Object> categories = asList("Italian", "Indian", "Chinese");
        List<Object> selectedCategories = Collections.emptyList();

        assertEquals(asList(0L, 0L, 0L), new OneHotEncoding().oneHotEncoding(categories, selectedCategories));
    }

    @Test
    public void moreThanOneSelected() {
        List<Object> categories = asList("Italian", "Indian", "Chinese");
        List<Object> selectedCategories = Arrays.asList("Italian", "Chinese");

        assertEquals(asList(1L, 0L, 1L), new OneHotEncoding().oneHotEncoding(categories, selectedCategories));
    }

    @Test
    public void allSelected() {
        List<Object> categories = asList("Italian", "Indian", "Chinese");
        List<Object> selectedCategories = Arrays.asList("Italian", "Chinese", "Indian");

        assertEquals(asList(1L, 1L, 1L), new OneHotEncoding().oneHotEncoding(categories, selectedCategories));
    }

    @Test
    public void nonExistentSelected() {
        List<Object> categories = asList("Italian", "Indian", "Chinese");
        List<Object> selectedCategories = Collections.singletonList("British");

        assertEquals(asList(0L, 0L, 0L), new OneHotEncoding().oneHotEncoding(categories, selectedCategories));
    }

    @Test
    public void oneNonExistentSelected() {
        List<Object> categories = asList("Italian", "Indian", "Chinese");
        List<Object> selectedCategories = Arrays.asList("British", "Chinese");

        assertEquals(asList(0L, 0L, 1L), new OneHotEncoding().oneHotEncoding(categories, selectedCategories));
    }

}