package org.cgiar;

import com.google.common.collect.Lists;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.yaml.snakeyaml.Yaml;
import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App
{
    static String d = File.separator;
    static final String NASA_POWER_API_URL = "https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M,T2M_MIN,T2M_MAX,T2MDEW,RH2M,PRECTOT,WS2M,ALLSKY_SFC_SW_DWN&community=AG&longitude={longitude}&latitude={latitude}&start={start_date}&end={end_date}&format=ICASA";

    public static void main( String[] args ) throws FileNotFoundException
    {

        // Load the YAML file
        Yaml yaml = new Yaml();
        FileInputStream inputStream = new FileInputStream("."+d+"config.yml");
        Map<String, Object> config = yaml.load(inputStream);
        String weatherLocationFile = (String)config.get("weatherLocationFile");
        int yearFrom = (Integer)config.get("yearFrom");
        int yearTo = (Integer)config.get("yearTo");
        String countryCode = (String)config.get("countryCode");
        int numberOfThreads = (Integer)config.get("numberOfThreads");

        // List of locations to fetch the weather data
        Object[] locations = getLocationInfo(weatherLocationFile, countryCode);
        System.out.println("> Number of locations: "+locations.length);

        // Concurrency
        try
        {
            ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
            for (Object location: locations)
            {
                Object[] o = (Object[])location;
                executor.submit(new CallAPI(o, yearFrom, yearTo));
            }
            executor.shutdown();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }

    }


    // Read the location information data from the input CSV file
    public static Object[] getLocationInfo(String weatherLocationFile, String countryCode)
    {
        List<Object> locationInfo = Lists.newArrayList();
        try
        {
            Reader in = new FileReader("." + d + "res" + d + weatherLocationFile);
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
            for (CSVRecord record : records)
            {

                //"CELL30M","X","Y","RASTERVALU","GT90PCT","ISO3"
                int cellId = Integer.parseInt(record.get("CELL30M"));
                String x = record.get("X");
                String y = record.get("Y");
                String iso3 = record.get("ISO3");

                // Limiting to the country code
                if (countryCode.equalsIgnoreCase("ALL"))
                {
                    // Putting all unit information in one object array
                    Object[] o = new Object[4];
                    o[0]  = cellId;
                    o[1]  = x;
                    o[2]  = y;
                    o[3]  = iso3;
                    locationInfo.add(o);
                }
                else if (!countryCode.isBlank() && countryCode.contains(iso3))
                {
                    // Putting all unit information in one object array
                    Object[] o = new Object[4];
                    o[0]  = cellId;
                    o[1]  = x;
                    o[2]  = y;
                    o[3]  = iso3;
                    locationInfo.add(o);
                }
            }
        }
        catch (Exception e) { e.printStackTrace(); }
        return locationInfo.toArray(Object[]::new);
    }



}
