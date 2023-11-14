package org.example.utils;

import com.google.gson.Gson;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class DataHelper {
    public static double getPriceOfOrder(String orderStr){
        Gson json = new Gson();
        Map<String, Object> order = json.fromJson(orderStr, HashMap.class);
        return (Double)order.get("price");
    }

    public static String getProductName(String orderStr){
        Gson json = new Gson();
        Map<String, Object> order = json.fromJson(orderStr, HashMap.class);
        return order.get("product").toString();
    }

    public static Tuple2<String, Double> getTuple(String orderStr){
        return new Tuple2<>(DataHelper.getProductName(orderStr), DataHelper.getPriceOfOrder(orderStr));
    }
}