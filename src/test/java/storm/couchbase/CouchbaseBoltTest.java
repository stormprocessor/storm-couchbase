package storm.couchbase;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import static org.junit.Assert.*;
import org.junit.*;

import java.net.URI;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: Michael Holt
 * Date: 1/25/13
 * Time: 10:26 PM
 */
public class CouchbaseBoltTest  {
    LinkedList <URI> uris = new LinkedList<URI>();
    String fakeBucket = "fake";
    String fakePassword = "pass";

    private CouchbaseBolt testCouchBolt = new CouchbaseBolt(uris, fakeBucket, fakePassword) {
        @Override
        public boolean shouldActOnInput(Tuple input) {
            return false;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getJSONForInput(Tuple input) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getKey(Tuple input) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    };

    @Before
    public void testSetup() {

    }

    @Test
    public void getExpireTimeTest() {
        assertTrue(testCouchBolt.getExpireTime() == 0);
    }

    @Test
    public void setExpireTimeTest() {
        testCouchBolt.setExpireTime(5000);
        assertTrue(testCouchBolt.getExpireTime() == 5000);
    }

}
