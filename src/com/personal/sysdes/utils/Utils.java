package com.personal.sysdes.utils;

public class Utils {

    public static int getRandomInt(int Min, int Max) {
        return Min + (int)(Math.random() * ((Max - Min) + 1));
    }
    
}
