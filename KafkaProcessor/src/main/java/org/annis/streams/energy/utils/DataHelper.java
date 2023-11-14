package org.annis.streams.energy.utils;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

public class DataHelper {
    public static double getPriceOfOrder(String orderStr){
        Gson json = new Gson();
        Map<String, Object> order = json.fromJson(orderStr, HashMap.class);
        return (Double)order.get("price");
    }
}
