/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.demo;

import smile.clustering.KMeans;
import smile.clustering.PartitionClustering;
import smile.data.DataFrame;
import smile.data.measure.NominalScale;
import smile.data.vector.DoubleVector;

/**
 *
 * @author top
 */
public class KMean {

    public KMeans train(DataFrame df) {
//        trainData = trainData.merge (IntVector.of ("PClassValues", encodeCategory (trainData, "Pclass")));
        df = df.merge(DoubleVector.of("MyTitle", encodeCategory(df, "Title")));
        df = df.merge(DoubleVector.of("MyCompany", encodeCategory(df, "Company")));

        double[][] data = new double[2][df.size()];
        for (int i = 0; i < df.size(); i++) {
            data[0][i] = (double) df.column("MyTitle").get(i);
            data[1][i] = (double) df.column("MyCompany").get(i);

        }
        return PartitionClustering.run(10, () -> KMeans.fit(data, 2));
    }

    public static double[] encodeCategory(DataFrame df, String columnName) {
        String[] values = df.stringVector(columnName).distinct().toArray(new String[]{});
        double[] pclassValues = df.stringVector(columnName).factorize(new NominalScale(values)).toDoubleArray();
        return pclassValues;
    }
}
