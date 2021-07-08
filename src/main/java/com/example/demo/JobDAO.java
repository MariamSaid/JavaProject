/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.demo;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import smile.data.DataFrame;
import smile.data.Tuple;
import smile.io.Read;

/**
 *
 * @author top
 */
public class JobDAO {

    public DataFrame readJobFromCSV(String fileName) {
        CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader();
        DataFrame df;
        try {
            df = (DataFrame) Read.csv(fileName, format);
        } catch (IOException | URISyntaxException e) {
            return null;
        }
        df.stream().distinct();

        return df.omitNullRows();
    }

    public List<Job> getJobList(DataFrame df) {
        assert df != null;
        List<Job> data = new ArrayList<>();
        ListIterator<Tuple> iterator = df.stream().collect(Collectors.toList()).listIterator();
        while (iterator.hasNext()) {
            Tuple t = iterator.next();
            Job j = new Job();
            j.setTitle((String) t.get("Title"));
            j.setCompany((String) t.get("Company"));
            j.setLocation((String) t.get("Location"));
            j.setType((String) t.get("Type"));
            j.setLevel((String) t.get("Level"));
            j.setYearsExp((String) t.get("YearsExp"));
            j.setCountry((String) t.get("Country"));
            j.setSkills((String) t.get("Skills"));
            data.add(j);
        }
        return data;
    }
          public List<String> factorizeYearExp(DataFrame df) {

        List<String> yearFactorized = new LinkedList<>();
        String[] YearsExp = df.apply("YearsExp").toStringArray();
        for (String data : YearsExp) {
            String splitted = data.split(" ")[0];
            if (splitted.indexOf('-') != -1) {
                String[] split = splitted.split("-");
                Integer i = (Integer.parseInt(split[0])+Integer.parseInt(split[1])) /2 ;
                yearFactorized.add(i.toString());
            } else if (splitted.indexOf('+') != -1) {
                yearFactorized.add(splitted.substring(0, splitted.indexOf('+')));
            }
             else  {
               yearFactorized.add("-1");  
            }
        }
        return(yearFactorized);
    }
}
