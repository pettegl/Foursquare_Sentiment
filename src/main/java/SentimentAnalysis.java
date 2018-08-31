import javafx.scene.control.TextInputDialog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


public class SentimentAnalysis
{
    final static String datasetLocation = "D:/TDT4305_Project/geotweets.tsv/";
    final static String positiveWords = "hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/positive-words.txt";
    final static String negativeWords = "hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/negative-words.txt";

    final static SparkConf conf = new SparkConf().setAppName("SentimentAnalysis").setMaster("local");
    final static JavaSparkContext sc = new JavaSparkContext(conf);


    final static Scanner scanner = new Scanner(System.in);


    final static JavaRDD<String> positiveDataset = sc.textFile(positiveWords);
    final static List<String> positiveGlossary = positiveDataset.collect();

    final static JavaRDD<String> negativeDataset = sc.textFile(negativeWords);
    final static List<String> negativeGlossary = negativeDataset.collect();

    final static HashMap<String, Integer> polarityMap = makeMap(positiveGlossary, negativeGlossary);


    final static int UTC_TIME = 0, COUNTRY_NAME = 1, COUNTRY_CODE = 2, PLACE_TYPE = 3, CITY_NAME = 4, LANGUAGE = 5,
            USERNAME = 6, USER_SCREEN_NAME = 7, USER_TIMEZONE_OFFSET = 8, NUMBER_OF_FRIENDS = 9, TEXT_OF_TWEET = 10,
            LAT = 11, LON = 12;

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

    static public JavaRDD<String> toLocalTime(JavaRDD<String> rdd)
    {
        return rdd.map(new Function<String, String>()
        {
            public String call(String s) throws Exception{
                String[] arr = s.split("\t");

                Calendar mydate = Calendar.getInstance();

                long localNumberOfSeconds = Long.parseLong(arr[UTC_TIME]) + (Long.parseLong(arr[USER_TIMEZONE_OFFSET])*60);

                mydate.setTimeInMillis(1000*localNumberOfSeconds);


                return arr[USER_SCREEN_NAME] + "\t" + getWeekday(mydate.get(Calendar.DAY_OF_WEEK));
            }
        });
    }

    // Loads the lexicons and maps them to a polarity.
    static public HashMap<String, Integer> makeMap(List<String> positiveGlossary, List<String> negativeGlossary)
    {
        HashMap<String, Integer> thisMap = new HashMap<String, Integer>();
        for(String s : positiveGlossary)
        {
            thisMap.put(s, 1);
        }
        for(String k : negativeGlossary)
        {
            thisMap.put(k, -1);
        }
        return thisMap;
    }

    // Gets the correct weekday
    static public String getWeekday(int day)
    {
        String dayString;
        switch (day) {
            case 1:
                dayString = "Monday";
                break;
            case 2:
                dayString = "Tuesday";
                break;
            case 3:
                dayString = "Wednesday";
                break;
            case 4:
                dayString = "Thursday";
                break;
            case 5:
                dayString = "Friday";
                break;
            case 6:
                dayString = "Saturday";
                break;
            case 7:
                dayString = "Sunday";
                break;
            default:
                dayString = "Invalid";
                break;
        }
        return dayString;

    }

    /*
    * Calculates polarity for all tweets. Returns an RDD with City, weekday and polarity
    * */
    static public JavaRDD<String> findPolarity(JavaRDD<String> rdd)
    {
        return rdd.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {

                // Split on tabs
                String[] arr = s.split("\t");

                // Split the tweet text by whitespaces
                String[] tweetArray = arr[TEXT_OF_TWEET].split(" ");
                int polarity = 0;

                for(String k : tweetArray)
                {
                    String m = k.replaceAll("[^a-zA-Z]+","");

                    if(polarityMap.get(m.toLowerCase()) != null) {
                        polarity += polarityMap.get(m.toLowerCase());
                    }
                }
                int finalPolarity = 0;

                if(polarity > 0)
                {
                    finalPolarity = 1;
                }
                else if(polarity == 0)
                {
                    finalPolarity = 0;
                }
                else if(polarity < 0)
                {
                    finalPolarity = -1;
                }

                Calendar mydate = Calendar.getInstance();
                /* Calculate local time from the unix timestamp. Time offset
                 is in minutes, so we multiply by 60 to get the additional number
                 of seconds to add or subtract.*/
                long localNumberOfSeconds = Long.parseLong(arr[UTC_TIME]) + (Long.parseLong(arr[USER_TIMEZONE_OFFSET])*60);

                // Multiply by 1000 to get timestamp in milliseconds.
                mydate.setTimeInMillis(1000*localNumberOfSeconds);

                // return object to RDD with columns city, weekday and polarity.

                return arr[CITY_NAME] + "\t" + getWeekday(mydate.get(Calendar.DAY_OF_WEEK)) + "\t" + finalPolarity;
            }
        });
    }

    // Filter on country, city and language, then map to pair using city+day as key.
    // Then reduceby that key and aggregate polarity.
    static public JavaPairRDD<String, Integer> filterMapAggregate(JavaRDD<String> rdd){
        return findPolarity(
                rdd.filter(s -> s.split("\t")[COUNTRY_CODE].equals("US") && s.split("\t")[PLACE_TYPE].equals("city") &&
                s.split("\t")[LANGUAGE].equals("en")))
                .mapToPair(s -> new Tuple2<>(s.split("\t")[0] + " " + s.split("\t")[1], Integer.parseInt(s.split("\t")[2])))
                .reduceByKey((a, b) -> a + b);
    }


    public static void main(String[] args) throws IOException {
        System.out.println("Type in path to dataset: ");
        JavaRDD<String> dataSet = removeRddHeader(sc.textFile(scanner.nextLine()));
        System.out.println("Write an output path ending with filename.txt/.tsv: ");
        String pathInput = scanner.nextLine();
        writeResultToFile(filterMapAggregate(dataSet), pathInput);



    }

    static public void writeResultToFile(JavaPairRDD<String, Integer> rdd, String path) throws IOException
    {
        BufferedWriter bw = new BufferedWriter(new FileWriter(path));

        TreeMap<String, Integer> map = new TreeMap<>(rdd.collectAsMap());

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            int l = entry.getValue();
            String s = entry.getKey();
            bw.write(s + "\t" + l);
            bw.newLine();
          }

        bw.close();
    }

}
