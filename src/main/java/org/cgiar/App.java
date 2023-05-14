package org.cgiar;

import com.google.common.collect.Lists;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.yaml.snakeyaml.Yaml;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import java.io.*;
import java.util.List;
import java.util.Map;

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

        // List of locations to fetch the weather data
        Object[] locations = getLocationInfo(weatherLocationFile, countryCode);
        OkHttpClient client = new OkHttpClient();

        // Converter
        IcasaToDssatWth itdw = new IcasaToDssatWth();

        // Looping through the locations one by one
        for (Object location: locations)
        {
            Object[] o = (Object[]) location;
            int cellId = (int)(o[0]);
            String longitude = (String)o[1];    //X
            String latitude = (String)o[2];     //Y
            String iso3 = (String)o[3];

            // Accumulated outputs
            StringBuilder icasa = new StringBuilder();

            // Loop over the years from 2000 to the present
            for (int year = yearFrom; year <= yearTo; year++)
            {
                String start_date = year + "0101";
                String end_date = year + "1231";

                String apiUrl = NASA_POWER_API_URL
                        .replace("{longitude}", longitude)
                        .replace("{latitude}", latitude)
                        .replace("{start_date}", start_date)
                        .replace("{end_date}", end_date);

                System.out.println("> Calling: "+apiUrl);

                Request request = new Request.Builder()
                        .url(apiUrl)
                        .build();

                try (Response response = client.newCall(request).execute())
                {
                    if (response.isSuccessful())
                    {
                        assert response.body() != null;
                        icasa.append(response.body().string());
                    }
                    else
                    {
                        System.err.println("> Error: " + response.message());
                    }
                }
                catch (IOException e)
                {
                    System.err.println("> Error: " + e.getMessage());
                }
            }

            // Convert ICASA to WTH
            String wth = itdw.convert(cellId, iso3, icasa.toString());

            // Write the accumulated output file
            String wthFileName = "." + d + "out" + d + iso3 + "_" + cellId + "_" + yearFrom + "-" + yearTo + ".wth";
            System.out.println("> Writing: "+wthFileName);
            try
            {
                BufferedWriter writer = new BufferedWriter(new FileWriter(wthFileName));
                writer.write(wth);
                writer.close();
            }
            catch (IOException ex)
            {
                System.out.println("> Skipping a file due to the locked file exception...");
            }

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
                if (!countryCode.isBlank() && countryCode.contains(iso3))
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
