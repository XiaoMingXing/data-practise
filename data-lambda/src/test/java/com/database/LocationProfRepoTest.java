package com.database;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class LocationProfRepoTest {


    @Test
    public void shouldCreateRepo() throws IOException {

        LocationProfRepo locationProfRepo = new LocationProfRepo();
        locationProfRepo.initialize();
    }
}