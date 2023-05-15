package org.cgiar;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;

public class CallAPI implements Callable<Object>
{

    int cellId;
    String longitude;    //X
    String latitude;     //Y
    String iso3;
    int yearFrom;
    int yearTo;
    OkHttpClient client = new OkHttpClient();
    IcasaToDssatWth itdw = new IcasaToDssatWth();

    CallAPI(Object[] o, int yearFrom, int yearTo)
    {
        this.cellId = (int)(o[0]);
        this.longitude = (String)o[1];    //X
        this.latitude = (String)o[2];     //Y
        this.iso3 = (String)o[3];
        this.yearFrom = yearFrom;
        this.yearTo = yearTo;
    }

    @Override
    public Object call()
    {

        // Accumulated outputs
        StringBuilder icasa = new StringBuilder();

        // Loop over the years from 2000 to the present
        for (int year = yearFrom; year <= yearTo; year++)
        {
            String start_date = year + "0101";
            String end_date = year + "1231";

            String apiUrl = App.NASA_POWER_API_URL
                    .replace("{longitude}", longitude)
                    .replace("{latitude}", latitude)
                    .replace("{start_date}", start_date)
                    .replace("{end_date}", end_date);

            //System.out.println("> Calling: "+apiUrl);

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
        String wthFileName = "." + App.d + "out" + App.d + iso3 + "_" + cellId + "_" + yearFrom + "-" + yearTo + ".wth";
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
        return wthFileName;
    }
}
