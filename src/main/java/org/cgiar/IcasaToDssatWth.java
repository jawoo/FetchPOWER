package org.cgiar;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.regex.Pattern;

public class IcasaToDssatWth
{

    static Pattern pattern = Pattern.compile("-?\\d+(\\.\\d+)?");
    static DecimalFormat dfXY   = new DecimalFormat("+000.0;-000.0");
    static DecimalFormat dfSRAD = new DecimalFormat("000.0");
    static DecimalFormat dfTEMP = new DecimalFormat("+00.0;-00.0");
    static DecimalFormat dfELEV = new DecimalFormat("00000");

    public String convert(int cellId, String iso3, String icasa)
    {
        StringBuilder wth = new StringBuilder();
        ArrayList<Object> dailyWeather = new ArrayList<>();
        Scanner s = new Scanner(icasa);
        boolean headerFound = false;
        double latitude = 0.0;
        double longitude = 0.0;
        double elevation = 0.0;
        double tav = 0.0;
        double amp = 0.0;
        int refht = 0;
        int wndht = 0;

        // Storing all values
        while (s.hasNextLine())
        {
            String line = s.nextLine();
            String[] values = line.trim().split("\\s+");

            // Header row
            if (values[0].equals("NASA"))
            {
                latitude = Double.parseDouble(values[1]);
                longitude = Double.parseDouble(values[2]);
                elevation = Double.parseDouble(values[3]);
                tav = Double.parseDouble(values[4]);
                amp = Double.parseDouble(values[5]);
                refht = Integer.parseInt(values[6]);
                wndht = Integer.parseInt(values[7]);
                headerFound = true;
            }

            // Data rows
            if (headerFound && isNumeric(values[0]))
            {
                String yyyyddd = values[0];
                double tmin = Double.parseDouble(values[2]);
                double tmax = Double.parseDouble(values[3]);
                double tdew = Double.parseDouble(values[4]);
                double rhum = Double.parseDouble(values[5]);
                double rain = Double.parseDouble(values[6]);
                double wind = Double.parseDouble(values[7]);
                String sradToCheck = values[8];
                double srad;

                // Check if SRAD is valid
                if (isNumeric(sradToCheck))
                    srad = Double.parseDouble(sradToCheck);
                else
                {
                    double dat = Double.parseDouble(yyyyddd.substring(4));
                    srad = estimateSRAD(latitude, dat, tmax, tmin);
                }

                Object[] o = new Object[8];
                o[0] = yyyyddd.substring(2);
                o[1] = srad;
                o[2] = tmax;
                o[3] = tmin;
                o[4] = rain;
                o[5] = tdew;
                o[6] = wind;
                o[7] = rhum;
                dailyWeather.add(o);

            }
        }
        s.close();

        // Write the data into DSSAT format
        if (headerFound && dailyWeather.size()>0)
        {
            String header = "*WEATHER DATA : CELL30M=" + cellId +", ISO3=" + iso3 + "\n" +
                    "\n" +
                    "@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT\n" +
                    "  NASA   {lati}   {long} {elv} {tav} {amp} {rht} {wht}\n" +
                    "@DATE  SRAD  TMAX  TMIN  RAIN  DEWP  WIND   PAR  EVAP  RHUM\n";
            wth = new StringBuilder(header
                    .replace("{lati}", dfXY.format(latitude))
                    .replace("{long}", dfXY.format(longitude))
                    .replace("{elv}", dfELEV.format(elevation))
                    .replace("{tav}", dfTEMP.format(tav))
                    .replace("{amp}", dfTEMP.format(amp))
                    .replace("{rht}", dfELEV.format(refht))
                    .replace("{wht}", dfELEV.format(wndht)));
            for (Object value : dailyWeather)
            {
                Object[] o = (Object[]) value;
                wth.append(o[0])
                    .append(" ").append(dfSRAD.format(o[1]))
                    .append(" ").append(dfTEMP.format(o[2]))
                    .append(" ").append(dfTEMP.format(o[3]))
                    .append(" ").append(dfSRAD.format(o[4]))
                    .append(" ").append(dfTEMP.format(o[5]))
                    .append(" ").append(dfSRAD.format(o[6]))
                    .append("             ").append(dfSRAD.format(o[7])).append("\n");
            }

        }
        return wth.toString();
    }

    // Numeric value?
    static boolean isNumeric(String strNum)
    {
        if (strNum == null) {
            return false;
        }
        return pattern.matcher(strNum).matches();
    }


    // In case the POWER-estimated SRAD is nan
    public static double estimateSRAD(double latitude, double dat, double tmax, double tmin)
    {
        double k1 = 0.174551;
        double phi = latitude * Math.PI / (double)180;
        double del = (23.45*Math.PI/(double)180)*Math.sin((double)2*Math.PI*((double)284+dat)/(double)365);
        double ws = Math.acos(-Math.tan(phi)*Math.tan(del));
        double df = (double)1+0.033*Math.cos((double)2*Math.PI*(dat/(double)365));
        double sc = 1.94;  // Solar constant, adjusted for 60 units embedded in the 1440 multiplier
        double ra = ((double)1440/Math.PI) * sc *df*Math.cos(phi)*Math.cos(del)*Math.sin(ws)+ws*Math.sin(phi)*Math.sin(del);
        double rs = k1*ra*Math.sqrt(tmax-tmin);
        return rs*0.041868;
    }

}
