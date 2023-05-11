package org.cgiar;

import com.google.gson.Gson;
import org.yaml.snakeyaml.Yaml;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public class App
{
    static String d = File.separator;
    static final String NASA_POWER_API_URL = "https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M,T2M_MIN,T2M_MAX,T2MDEW,RH2M,PRECTOT,WS2M,ALLSKY_SFC_SW_DWN&community=AG&longitude={longitude}&latitude={latitude}&start={start_date}&end={end_date}&format=ICASA";

    public static void main( String[] args ) throws FileNotFoundException {

        // Load the YAML file
        Yaml yaml = new Yaml();
        FileInputStream inputStream = new FileInputStream("."+d+"config.yml");
        Map<String, Object> config = yaml.load(inputStream);
        String weatherLocationsCSV = (String)config.get("weatherLocationsCSV");
        int yearFrom = (Integer)config.get("yearFrom");
        int yearTo = (Integer)config.get("yearTo");
        String countryCode = (String)config.get("countryCode");

        String longitude = "-77.0300";
        String latitude = "38.8900";

        OkHttpClient client = new OkHttpClient();
        Gson gson = new Gson();

        // Loop over the years from 2000 to the present
        for (int year = yearFrom; year <= yearTo; year++) {
            String start_date = year + "0101";
            String end_date = year + "1231";

            String apiUrl = NASA_POWER_API_URL
                    .replace("{longitude}", longitude)
                    .replace("{latitude}", latitude)
                    .replace("{start_date}", start_date)
                    .replace("{end_date}", end_date);

            Request request = new Request.Builder()
                    .url(apiUrl)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (response.isSuccessful()) {
                    String json = response.body().string();
                    // Parse and handle the JSON response as needed
                    // For example, you could print it to the console:
                    System.out.println(json);
                } else {
                    System.err.println("Error: " + response.message());
                }
            } catch (IOException e) {
                System.err.println("Error: " + e.getMessage());
            }
        }
    }
}
