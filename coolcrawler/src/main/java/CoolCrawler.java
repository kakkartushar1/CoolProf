import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import twitter4j.*;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;


/**
 * Created by rubcuevas on 05/11/14.
 */
public class CoolCrawler {

    private static MongoClient mongoClient;
    private static DBCollection collection;
    private static Logger logger = LogManager.getLogger();
    private static String STOPWORDS_ES_FILE;
    private static String DB;
    private static String COLLECTION;


    public static void main(String args[]) {
        try {
            STOPWORDS_ES_FILE = args[0];
            DB = args[1];
            COLLECTION = args[2];

            mongoClient = new MongoClient("localhost", 27017);
            collection = mongoClient.getDB(DB).getCollection(COLLECTION);

            StatusListener listener = new StatusListener() {


            @Override
            public void onStatus(Status status) {

                //Filter Spanish tweets
                if (status.getLang().equals("es")) {
                    String json = TwitterObjectFactory.getRawJSON(status);
                    DBObject dbObject = (DBObject) JSON.parse(json);
                    collection.insert(dbObject);
                }

            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                logger.warn("Got a status deletion notice id: " + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                logger.warn("Got track limitation notice: " + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long l, long l2) {
                logger.warn("Got scrub_geo event. l: " + l + "l2: " + l2);
            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {
                logger.error("There was a stallWarning:\n" + stallWarning.getMessage());
            }

            @Override
            public void onException(Exception ex) {
                logger.error("An exception has been thrown:\n" + ex.getMessage());


            }
        };
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        FilterQuery filter = new FilterQuery();

        filter.track(getStopWords());
        twitterStream.filter(filter);

    } catch (UnknownHostException e) {
            e.printStackTrace();
        }

    }

    private static String[] getStopWords(){
        try {
            Stream<String> lines = Files.lines(Paths.get(STOPWORDS_ES_FILE));
            return lines.toArray(size -> new String[size]);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}


