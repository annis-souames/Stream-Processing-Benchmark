package org.annis.streams.energy.generators;

import org.annis.streams.energy.generators.DataGenerator;

public class RunStoreB {
    public static void main(String[] args) throws InterruptedException {
        final int RATE = 10(000;
        final int DURATION = 5*60; // In seconds

        DataGenerator gen = new DataGenerator("orders-b", RATE);
        for(int i = 0 ; i < DURATION ; i++){
            gen.generateOrders();
        }

    }
}
