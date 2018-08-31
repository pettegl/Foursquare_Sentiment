import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.IntFunction;

public class FourSquare {

    final static double R = 6372.8;
    final static String datasetLocation = "D:/TDT4305/dataset_TIST2015.tsv";
    final static String citiesDatasetLocation = "D:/TDT4305/dataset_TIST2015_Cities.txt";
    final static SparkConf conf = new SparkConf().setAppName("SparkTest").setMaster("local");
    final static JavaSparkContext sc = new JavaSparkContext(conf);
    final static JavaRDD<String> dataset = removeRddHeader(sc.textFile(datasetLocation));
    final static JavaRDD<String> citiesDataset = sc.textFile(citiesDatasetLocation);
    final static JavaRDD<String> datasetWithCities = assignCities();
    final static int CHECKIN_ID = 0, USER_ID = 1, SESSION_ID = 2, UTC_TIME = 3, TIMEZONE_OFFSET = 4, LAT = 5,
              LON = 6, CATEGORY = 7, SUBCATEGORY = 8, CITY = 9, COUNTRY = 10;


    /*Remove the first row which isn't a legit check-in.*/
    static private JavaRDD<String> removeRddHeader(JavaRDD<String> rdd){
        return rdd.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                if(Character.isDigit(s.charAt(0))){
                    return true;
                }else{
                    return false;
                }
            }
        });
    }

    /*Task 2*/
    static public JavaRDD<String> toLocalTime(){
        return dataset.map(new Function<String, String>() {
            public String call(String s) throws Exception{
                String[] arr = s.split("\t");
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date currentDate = df.parse(arr[UTC_TIME]);
                currentDate.setMinutes(currentDate.getMinutes() + Integer.parseInt(arr[4]));

                arr[UTC_TIME] = df.format(currentDate);
                return String.join("\t", arr);
            }
        });
    }

    /*Task 3*/
    static private JavaRDD<String> assignCities() {
        final List<String> cities = citiesDataset.collect();
        return dataset.map(new Function<String, String>() {
            public String call(String s) throws Exception {

                ArrayList<Double> distances = new ArrayList<Double>();

                String[] arr = s.split("\t");
                // Get coordinates to check all cities with
                double lat1 = Double.parseDouble(arr[5]), lon1 = Double.parseDouble(arr[6]);

                // For each city, calculate distance from lat1+lon1 to the distance to the city
                for (String cityString : cities)
                {
                    String[] arr2 = cityString.split("\t");

                    double lat2 = Double.parseDouble(arr2[1]), lon2 = Double.parseDouble(arr2[2]);

                    double finalDistance = haverSine(lat1, lon1, lat2, lon2);

                    distances.add(finalDistance);

                    // The index where the lowest double in distances is, will be the object at cities.get(k).
                    // To get city we split this object, and city will be at arr[0] and country at arr[4].

                }
                // Finds the index of the smallest distance.
                double min = Collections.min(distances);
                int minIndex = distances.indexOf(min);
                // Locates the city with the closest distance
                String[] arr3 = cities.get(minIndex).split("\t");

                // Appends it to the object tail. Return an RDD with checkin_id, city and country.
                return s + "\t" + arr3[0] + "\t" + arr3[4];

            }
        });
    }

    /*Task 4A*/
    static public long countUniqueUsers(){
        return dataset.map(new Function<String, String>() {
            public String call(String s) throws Exception {
                return s.split("\t")[USER_ID];
            }
        }).distinct().count();
    }

    /*Task 4B*/
    static public long countUniqueCheckins(){
        return dataset.map(new Function<String, String>() {
            public String call(String s) throws Exception {
                return s.split("\t")[CHECKIN_ID];
            }
        }).distinct().count();
    }

    /*Task 4C*/
    static public long countUniqueSessions(){
        return dataset.map(new Function<String, String>() {
            public String call(String s) throws Exception {
                return s.split("\t")[SESSION_ID];
            }
        }).distinct().count();
    }

    /*Task 4D*/
    static public long countUniqueCountries(){
        return datasetWithCities.map(new Function<String, String>() {
            public String call(String s) throws Exception {
                return s.split("\t")[COUNTRY]; //Position of countries in string
            }
        }).distinct().count();
    }

    /*Task 4E*/
    static public long countUniqueCities(){
        return datasetWithCities.map(new Function<String, String>() {
            public String call(String s) throws Exception {
                String[] arr = s.split("\t");
                return arr[CITY] + arr[COUNTRY]; //Position of cities in string
            }
        }).distinct().count();
    }

    /*Task 5*/
    static public JavaPairRDD<String, Integer> numOfCheckinPerSession(){
        return dataset.mapToPair(s -> new Tuple2<String, Integer>(s.split("\t")[SESSION_ID], 1)).reduceByKey((a, b) -> a + b);
    }

    static public JavaPairRDD<Integer, Integer> numOfSessionsPerCheckinCount(){
        return numOfCheckinPerSession().mapToPair(s -> new Tuple2<Integer, Integer>(s._2(), 1)).reduceByKey((a, b) -> a + b);
    }
    /*Task 6*/
    static public JavaPairRDD getSessionDistanceRDD(){
        return dataset.groupBy(checkIn -> checkIn.split("\t")[SESSION_ID])
                .filter(amountOfCheckins -> getIteratorLength(amountOfCheckins._2()) >= 4)
                .mapToPair(sessionDistance -> new Tuple2<>(sessionDistance._1(), getSessionDistance(sessionDistance._2())));
    }

    /*Task 7*/
    static public JavaRDD getSortedSessionsLongerThan50km(){
        return dataset.groupBy(checkIn -> checkIn.split("\t")[SESSION_ID])
                .filter(amountOfCheckins -> getIteratorLength(amountOfCheckins._2()) >= 4)
                .mapToPair(sessionDistance -> new Tuple2<>(getIteratorLength(sessionDistance._2()), new Tuple2<>(getSessionDistance(sessionDistance._2()), sessionDistance._2())))
                .sortByKey(false)
                .filter(session -> session._2()._1() >= 50)
                .map(s -> s._2()._2());
    }

    static public void getCheckinsOf100LongestSessionsOver50km() throws IOException{
        List l  = getSortedSessionsLongerThan50km().take(100);
        Iterator iter = l.iterator();
        BufferedWriter bw = new BufferedWriter(new FileWriter("out.txt"));
        while(iter.hasNext()){
            bw.write(iter.next().toString());
            bw.newLine();
        }
        bw.close();
    }

    static private double getSessionDistance(Iterable<String> checkins){
        double lat1 = 0.0;
        double lon1 = 0.0;
        boolean first = true;
        double distance = 0;
        for (String checkIn : checkins){
            String[] splittedLine = checkIn.split("\t");
            if (first){
                lat1 = Double.parseDouble(splittedLine[LAT]);
                lon1 = Double.parseDouble(splittedLine[LON]);
                first = false;
                continue;
            }else{
                double lat2 = Double.parseDouble(splittedLine[LAT]);
                double lon2 = Double.parseDouble(splittedLine[LON]);
                distance += haverSine(lat1,lon1,lat2,lon2);
                lat1=lat2;
                lon1=lon2;
            }
        }
        return distance;
    }

    static private int getIteratorLength(Iterable<String> s){
        int count = 0;
        for(String checkin : s){
            count++;
        }
        return count;
    }

    /*Helper method to write RDD to file*/
    static private Boolean writeToFile(JavaRDD<String> rdd, int numberOfTakes) throws IOException
    {
        BufferedWriter bw = new BufferedWriter(new FileWriter("out.txt"));
        try{
            if(numberOfTakes == 0)
            {
                List<String> rddList = rdd.collect();
                for (String s : rddList) {
                    bw.write(s);
                    bw.newLine();
                }
                bw.close();
                return true;
            }
            else if(numberOfTakes > 0)
            {
                List<String> rddList = rdd.take(numberOfTakes);
                for (String s : rddList)
                {
                    bw.write(s);
                    bw.newLine();
                }
                bw.close();
                return true;
            }
            else
            {
                throw new IllegalArgumentException("Slutt.");
            }
        } catch(NullPointerException e)
        {
            e.printStackTrace();
        }
        return false;
    }

    private static void printSessions(TreeMap<Integer, Integer> tree) throws IOException
    {

        BufferedWriter bw = new BufferedWriter(new FileWriter("Histogram.txt"));
        try
        {
            for(int i = 0; i <= tree.size(); i++)
            {
                if(tree.get(i) != null)
                {
                    bw.write(i + ", " + tree.get(i));
                    bw.newLine();
                }

            }
            bw.close();

        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
    }
    private static Double haverSine(double lat1, double lon1, double lat2, double lon2)
    {
        Double latDistance = toRad(lat2-lat1);
        Double lonDistance = toRad(lon2-lon1);
        Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) +
                Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
                        Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return (R * c);
    }
    private static Double toRad(Double value) {
        return value * Math.PI / 180;
    }



}