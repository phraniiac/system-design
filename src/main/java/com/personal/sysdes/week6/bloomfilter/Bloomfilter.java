package com.personal.sysdes.week6.bloomfilter;

import org.apache.commons.codec.digest.MurmurHash3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Bloomfilter {

    private final int[] bf;
    private final int sz;
    private final int numHash;

    public Bloomfilter(int sz, int numHash) {
        this.numHash = numHash;
        this.sz = sz;
        this.bf = new int[sz];
    }

    private int performHash(String ele, int seed) {
        int res = MurmurHash3.hash32(ele.getBytes(), ele.getBytes().length, seed);
        res = (res % this.sz);
        if (res < 0) {
            res += this.sz;
        }
        return res ;
    }

    public void addEle(String ele) {
        for (int i = 0; i < this.numHash; i++) {
            this.bf[performHash(ele, i)] = 1;
        }
    }

    public boolean checkEle(String ele) {
        for (int i = 0; i < this.numHash; i++) {
            if(this.bf[performHash(ele, i)] == 0) {
                return false;
            }
        }
        return true;
    }


    public static void main(String[] args) {
        Bloomfilter bf = new Bloomfilter(100, 5);

        List<String> animalsPresent = new ArrayList<>(Arrays.asList("dog", "cat", "giraffe", "fly", "mosquito", "horse", "eagle",
                "bird", "bison", "boar", "butterfly", "ant", "anaconda", "bear",
                "chicken", "dolphin", "donkey", "crow", "crocodile"));

        for (String animal: animalsPresent) {
            bf.addEle(animal);
        }


        for (String animal: animalsPresent) {
            if (bf.checkEle(animal)) {
                System.out.println("Expected animal is Present!!");
            }
            else {
                System.out.println("False negative??????");
                throw new IllegalStateException();
            }
        }

        List<String> otherAnimals = new ArrayList<>(Arrays.asList("badger", "cow", "pig", "sheep", "bee", "wolf", "fox",
                "whale", "shark", "fish", "turkey", "duck", "dove",
                "deer", "elephant", "frog", "falcon", "goat", "gorilla",
                "hawk"));

        for (String animal: otherAnimals) {
            if (bf.checkEle(animal)) {
                System.out.println("False Positive,..");
            }
            else {
                System.out.println("Expected animal is not Present!!");
            }
        }

    }
}
