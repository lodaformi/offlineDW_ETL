package com.loda.geo;

import ch.hsr.geohash.GeoHash;

/**
 * @Author loda
 * @Date 2023/3/8 17:16
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class GeoTest {
    public static void main(String[] args) {
        Double lng1 = 95.11045987748628;
        Double lat1 = 40.18365597944445;

        Double lng2 = 94.75011840486005;
        Double lat2= 40.25516465387142;

        System.out.println(GeoHash.withCharacterPrecision(lat1, lng1, 6));
        System.out.println(GeoHash.withCharacterPrecision(lat2, lng2, 6));
        String s = GeoHash.withCharacterPrecision(lat1, lng1, 6).toBase32();
        System.out.println(GeoHash.withCharacterPrecision(lat1, lng1, 6).toBase32());
        System.out.println(GeoHash.withCharacterPrecision(lat2, lng2, 6).toBase32());
    }
}
