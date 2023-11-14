package org.example;

import java.io.Serializable;

public class Order implements Serializable {
        private String product;
        private double price;

        public Order(String product, Double price){
            this.product = product;
            this.price = price;
        }

        public String getProduct() {
            return product;
        }

        public void setProduct(String product) {
            this.product = product;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "product='" + this.product + '\'' +
                    ", price=" + this.price +
                    '}';
        }
    }

