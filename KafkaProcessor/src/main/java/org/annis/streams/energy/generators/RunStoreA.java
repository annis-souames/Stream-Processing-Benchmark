package org.annis.streams.energy.generators;

import org.annis.streams.energy.generators.DataGenerator;

public class RunStoreA {
    public static void main(String[] args) throws InterruptedException {
        final int RATE = 100000;
        final int DURATION = 5*60; // In seconds

        DataGenerator gen = new DataGenerator("orders-a", RATE);
        for(int i = 0 ; i < DURATION ; i++){
            gen.generateOrders();
        }

    }
}
