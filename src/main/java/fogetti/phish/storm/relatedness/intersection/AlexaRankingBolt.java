package fogetti.phish.storm.relatedness.intersection;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SignatureException;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class AlexaRankingBolt extends AbstractRedisBolt {

    private static final long serialVersionUID = -8497557061656398615L;
    private static final Logger logger = LoggerFactory.getLogger(AlexaRankingBolt.class);
    private static final String SERVICE_HOST = "awis.amazonaws.com";
    private static final String AWS_BASE_URL = "http://" + SERVICE_HOST + "/?";
    private final String accessKey;
    private final String secretKey;
    private Encoder encoder;
    private Decoder decoder;
    private OutputCollector collector;
    
    public AlexaRankingBolt(JedisPoolConfig config, String accessKey, String secretKey) {
        super(config);
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.collector = collector;
        this.encoder = Base64.getEncoder();
        this.decoder = Base64.getDecoder();
    }

    @Override
    public void execute(Tuple input) {
        String URL = getURL(input);
        logger.info("Ranking URL [{}]", URL);
        String ranking = initRanking(URL);
        logger.info("Alexa ranking [{}]", ranking);
        if (StringUtils.isBlank(ranking)) {
            collector.fail(input);
        } else {
            cacheRanking(URL, ranking);
            collector.emit(input, new Values(encode(URL), ranking));
            collector.ack(input);
        }
    }

    protected String getURL(Tuple input) {
        byte[] decoded = decoder.decode(input.getStringByField("url"));
        String URL = new String(decoded, StandardCharsets.UTF_8);
        return URL;
    }
    
    private String encode(String URL) {
        String encodedURL = encoder.encodeToString(URL.getBytes(StandardCharsets.UTF_8));
        return encodedURL;
    }
    
    private String initRanking(String URL) {
        String ranking = findCachedRanking(URL);
        try {
            if (StringUtils.isBlank(ranking)) {
                UrlInfo urlInfo = new UrlInfo(accessKey, secretKey, URL);
                String query = urlInfo.buildQuery();
                String toSign = "GET\n" + SERVICE_HOST + "\n/\n" + query;
                String signature = urlInfo.generateSignature(toSign);
                String uri = AWS_BASE_URL + query + "&Signature=" + URLEncoder.encode(signature, "UTF-8");
                String xmlResponse = urlInfo.makeRequest(uri);

                Document doc = Jsoup.parse(xmlResponse, "", Parser.xmlParser());
                Elements rank = doc.select("aws|Rank");
                if (!rank.isEmpty()) {
                    ranking = rank.text();
                } else {
                    ranking = "10000000";
                }
            }
        } catch (IOException e) {
            logger.error("Alexa ranking lookup failed", e);
        } catch (SignatureException e) {
            logger.error("Alexa ranking lookup failed", e);
        }
        return ranking;
    }
    
    private String findCachedRanking(String URL) {
        try (Jedis jedis = (Jedis) getInstance()) {
            String registeredURL = findRegisteredURL(URL);
            String ranking = jedis.get("alexa:"+encode(registeredURL));
            logger.info("Found Alexa ranking [{}] for URL [{}]", ranking, URL);
            return ranking;
        }
    }

    private String findRegisteredURL(String URL) {
        String[] urlParts = URL.split("//");
        String protocol = urlParts[0];
        String URLPostFix = urlParts[1];
        String URLPrefix = StringUtils.substringBefore(URLPostFix, "/");
        return protocol+"//"+URLPrefix;
    }

    private void cacheRanking(String URL, String ranking) {
        try (Jedis jedis = (Jedis) getInstance()) {
            String registeredURL = findRegisteredURL(URL);
            jedis.set("alexa:"+encode(registeredURL), ranking);
            logger.info("Cached Alexa ranking [{}] for URL [{}]", ranking, URL);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "ranking"));
    }

}
