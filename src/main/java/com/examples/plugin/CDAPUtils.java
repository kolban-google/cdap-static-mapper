package com.examples.plugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CDAPUtils {
    /**
     * Process a CDAP delimeter config value and return a list of maps.  Each map contains key/value pairs where
     * the keys are the column names and the values are taken from the config value.
     * 
     * For example, imagine we have (In CSV) records that look like:
     * Name  Age  Sex
     * Bob   40   M
     * Sue   52   F
     * Fred  34   M
     * 
     * This might be passed in encoded as:
     * 
     * Bob:40:M,Sue:52:F,Fred:34:M
     * 
     * The call to this function would have a column list of ["Name", "Age", "Sex"]
     * 
     * @param columnList A list of the columns in the data.
     * @param configValue A value of a configuration.
     * @return An list of maps where each map is keyed off the column names.
     */
    static List<Map<String, String>> parseDelimiters(List<String> columnList, String configValue) {
        ArrayList<Map<String,String>> list = new ArrayList<>();
        String entries[] = configValue.split(",");
        for (String entry: entries) {
            String values[] = entry.split(":");

            /*
            System.out.println("values: " + values);
            for (int i=0; i<values.length; i++) {
                System.out.println(values[i]);
            }
            */
            HashMap<String, String> map = new HashMap<>();
            for (int i=0; i<columnList.size(); i++) {
                map.put(columnList.get(i), i<values.length?values[i]:null);
            }
            list.add(map);
        }
        return list;
    } // parseDelimiters
}