package fogetti.phish.storm.relatedness;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.IReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.container.JedisCommandsContainerBuilder;
import org.apache.storm.redis.common.container.JedisCommandsInstanceContainer;
import org.apache.storm.redis.common.container.JedisContainer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Implementation of the following real time phishing classifier:
 * <a href="https://orbilu.uni.lu/bitstream/10993/20053/1/phishStorm-revised.pdf">
 * https://orbilu.uni.lu/bitstream/10993/20053/1/phishStorm-revised.pdf
 * </a>
 * @author gergely.nagy
 *
 */
public class URLSpout extends BaseRichSpout {

    public static final String SUCCESS_STREAM = "success";
    public static final String INTERSECTION_STREAM = "intersect";

    private static final long serialVersionUID = -6424905468176142975L;
	private static final Logger logger = LoggerFactory.getLogger(URLSpout.class);
	
	private SpoutOutputCollector collector;
	private TreeSet<String> urllist;
	private String urlDataFile;
	private JedisPoolConfig config;
    private Encoder encoder;
    private Decoder decoder;
    private String[] schemes = {"http","https"};
    private UrlValidator urlValidator;
    private final int METRICS_WINDOW = 10;
    private transient CountMetric ackedPublished;
    private transient CountMetric spoutAcked;
    private transient CountMetric spoutSkipped;
    private transient CountMetric spoutFailed;
    private transient CountMetric spoutEmitted;
    private transient ReducedMetric spoutListSize;
    private JedisContainer container;

	public URLSpout(String urlDataFile, JedisPoolConfig config) {
		this.urlDataFile = urlDataFile;
		this.config = config;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.urllist = readURLListFromFile();
        this.encoder = Base64.getEncoder();
        this.decoder = Base64.getDecoder();
        this.urlValidator = new UrlValidator(schemes);
        this.container = (JedisContainer)JedisCommandsContainerBuilder.build(config);
        ackedPublished = new CountMetric();
        context.registerMetric("acked-published",
                               ackedPublished,
                               METRICS_WINDOW);
        spoutAcked = new CountMetric();
        context.registerMetric("spout-acked",
                               spoutAcked,
                               METRICS_WINDOW);
        spoutSkipped = new CountMetric();
        context.registerMetric("spout-skipped",
                               spoutSkipped,
                               METRICS_WINDOW);
        spoutFailed = new CountMetric();
        context.registerMetric("spout-failed",
                               spoutFailed,
                               METRICS_WINDOW);
        spoutEmitted = new CountMetric();
        context.registerMetric("spout-emitted",
                               spoutEmitted,
                               METRICS_WINDOW);
        spoutListSize = new ReducedMetric(new URLListSizeReducer());
        context.registerMetric("spout-list-size",
                               spoutListSize,
                               METRICS_WINDOW);
	}

	private TreeSet<String> readURLListFromFile() {
	    TreeSet<String> urls = new TreeSet<>();
		loadUrls(urls);
		return urls;
	}

	private void loadUrls(Set<String> urls) {
		try(Scanner scanner = new Scanner(new FileReader(urlDataFile));) {
			while (scanner.hasNextLine()) {
				urls.add(scanner.nextLine());
			}
		} catch (FileNotFoundException e) {
		    logger.error("Could not load the URLs", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void nextTuple() {
	    spoutListSize.update(urllist.size());
		if (!urllist.isEmpty()) {
			String URLWithScheme = urllist.last();
			urllist.remove(URLWithScheme);
			if (!saved(URLWithScheme)) doNextTuple(URLWithScheme);
			else spoutSkipped.incr();
		}
	}

    private boolean saved(String longURL) {
        String encodedURL = getEncodedURL(longURL);
        String key = "saved:" + encodedURL;
        Jedis jedis = (Jedis) getInstance();
        try {
            List<String> messages = jedis.lrange(key, 0L, 0L);
            if (messages != null && !messages.isEmpty()) return true;
        } catch (JedisException e) {
            closeRedis();
            throw e;
        } finally {
            returnInstance(jedis);
        }
        return false;
    }

    private String getEncodedURL(String longURL) {
        byte[] message = longURL.getBytes(StandardCharsets.UTF_8);
        String encodedURL = encoder.encodeToString(message);
        return encodedURL;
    }

	private void doNextTuple(String URLWithScheme) {
	    String URL = URLWithScheme + "#" +System.currentTimeMillis();
        String encodedShortURL = encoder.encodeToString(URLWithScheme.getBytes(StandardCharsets.UTF_8));
        String encodedLongURL = encoder.encodeToString(URL.getBytes(StandardCharsets.UTF_8));
        logger.info("Emitting [{}]", encodedLongURL);
		collector.emit(new Values(encodedShortURL), encodedLongURL);
		spoutEmitted.incr();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
        declarer.declareStream(INTERSECTION_STREAM, new Fields("url"));
        declarer.declareStream(SUCCESS_STREAM, new Fields("url"));
	}

	@Override
	public void ack(Object encodedURL) {
	    String URL = getURL(encodedURL.toString());
	    logger.info("Acking [{}]", URL);
        if (URL.startsWith("result://")) {
	        spoutAcked.incr();
	        return;
	    }
        if (URL.startsWith("intersect://")) {
            String shortURL = StringUtils.removeStart(URL, "intersect://");
            ackRegular(shortURL);
            return;
        }
        ackIntersection(removeTail(URL));
	}

    private void ackIntersection(String URL) {
        logger.info("Intersecting [Message={}]", URL);
        String resmsg = "intersect://"+URL;
        String encodedURL = encoder.encodeToString(URL.getBytes(StandardCharsets.UTF_8));
        String encodedMsgId = encoder.encodeToString(resmsg.getBytes(StandardCharsets.UTF_8));
        collector.emit(INTERSECTION_STREAM, new Values(encodedURL), encodedMsgId);
        spoutAcked.incr();
    }

    private void ackRegular(String URL) {
        String encoded = encoder.encodeToString(URL.getBytes(StandardCharsets.UTF_8));
        publish(encoded);
        spoutAcked.incr();
    }

	@Override
	public void fail(Object encodedURL) {
	    try {
	        String resURL = stripURL(encodedURL);
	        logger.debug("Message [msg={}] failed", resURL);
	        if (urlValidator.isValid(resURL)) {
	            logger.warn("Requeueing [msg={}]", resURL);
	            urllist.add(resURL);
	        } else {
	            logger.warn("Skipping invalid URL [msg={}]", resURL);
	        }
	    } catch(IllegalArgumentException e) {
	        logger.error("["+encodedURL+"] failed", e);
	    }
		spoutFailed.incr();
	}

    private String stripURL(Object encodedURL) {
        String resURL = getURL(encodedURL.toString());
        if (resURL.startsWith("result://")) {
            resURL = getURL(StringUtils.removeStart(resURL, "result://"));
        } else if (resURL.startsWith("intersect://")) {
            resURL = StringUtils.removeStart(resURL, "intersect://");
        } else {
            resURL = removeTail(resURL);
        }
        return resURL;
    }

    private String getURL(String encodedURL) {
        byte[] decoded = decoder.decode(encodedURL);
        String longURL = new String(decoded, StandardCharsets.UTF_8);
        return longURL;
    }
    
    private String removeTail(String shortURL) {
        String URL = StringUtils.substringBeforeLast(shortURL, "#");
        return URL;
    }

    private void publish(String URL) {
        logger.info("Publishing [Message={}]", URL);
        String resmsg = "result://"+URL;
        String encodedMsg = encoder.encodeToString(resmsg.getBytes(StandardCharsets.UTF_8));
        collector.emit(SUCCESS_STREAM, new Values(URL), encodedMsg);
        ackedPublished.incr();
    }

    /**     
     * Borrow JedisCommands instance from container.<p></p>     
     * JedisCommands is an interface which contains single key operations.      
     * @return implementation of JedisCommands      
     * @see JedisCommandsInstanceContainer#getInstance()        
     */     
    protected JedisCommands getInstance() {     
        return this.container.getInstance();        
    }

    /**
     * Return borrowed instance to container.
     * @param instance borrowed object
     */
    protected void returnInstance(JedisCommands instance) {
        this.container.returnInstance(instance);
    }
    
    /**
     * Close the redis container.
     */
    private void closeRedis() {
        this.container.close();
    }
    
    private class URLListSizeReducer implements IReducer<Integer> {
        
        private Integer size;

        @Override
        public Integer init() {
            return 0;
        }

        @Override
        public Integer reduce(Integer accumulator, Object input) {
            if(input instanceof Integer) {
                size = (Integer)input;
            }
            return size;
        }

        @Override
        public Object extractResult(Integer accumulator) {
            return accumulator;
        }
        
    }
}
