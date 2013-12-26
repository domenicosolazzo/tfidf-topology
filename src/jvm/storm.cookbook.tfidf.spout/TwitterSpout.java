package storm.cookbook.tfidf.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TwitterSpout extends BaseRichSpout {

    Logger LOG = LoggerFactory.getLogger(TwitterSpout.class);
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream twitterStream;
    String[] trackTerms;
    long maxQueueDepth;
    SpoutOutputCollector collector;

    public TwitterSpout(String[] trackTerms, long maxQueueDepth){
        this.trackTerms = trackTerms;
        this.maxQueueDepth = maxQueueDepth;
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        queue = new LinkedBlockingQueue<Status>(1000);
        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
                if(queue.size() < maxQueueDepth){
                    LOG.trace("TWEET Received: " + status);
                    queue.offer(status);
                } else {
                    LOG.error("Queue is now full, the following message is dropped: "+status);
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onException(Exception e) {
            }

            @Override
            public synchronized void onStallWarning(StallWarning arg0) {
                LOG.error("Stall warning received!");
            }

        };
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        FilterQuery filter = new FilterQuery();
        filter.count(0);
        filter.track(trackTerms);
        twitterStream.filter(filter);

    }
    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        if(ret == null){
            try { Thread.sleep(50); } catch (InterruptedException e) {}
        } else {
            collector.emit(new Values(ret));
        }

    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }



}