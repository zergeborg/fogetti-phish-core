package fogetti.phish.storm.relatedness.intersection;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fogetti.phish.storm.client.WrappedRequest;
import okhttp3.OkHttpClient;
import okhttp3.Response;

public class AlexaRankingBolt extends BaseRichBolt {

    private static final long serialVersionUID = -8497557061656398615L;
    private static final Logger logger = LoggerFactory.getLogger(AlexaRankingBolt.class);
    private Encoder encoder;
    private Decoder decoder;
    private OutputCollector collector;
    private int connectTimeout = 5000;
    private int socketTimeout = 5000;
    private OkHttpClient.Builder builder;
    private String proxyDataFile;
    private List<String> proxyList;
    
    public AlexaRankingBolt(String proxyDataFile) {
        this.proxyDataFile = proxyDataFile;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.encoder = Base64.getEncoder();
        this.decoder = Base64.getDecoder();
        this.builder = buildClient();
        try {
            this.proxyList = Files.readAllLines(Paths.get(proxyDataFile));
        } catch (IOException e) {
            logger.error("Preparing the Google SEM bolt failed", e);
        }
    }

    private OkHttpClient.Builder buildClient() {
        OkHttpClient.Builder builder
            = new OkHttpClient
                .Builder()
                .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
                .readTimeout(socketTimeout, TimeUnit.MILLISECONDS)
                .writeTimeout(socketTimeout, TimeUnit.MILLISECONDS)
                .retryOnConnectionFailure(true)
                .followRedirects(true)
                .followSslRedirects(true);
        return builder;
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
        String ranking = "";
        try {
            builder.proxy(buildProxy());
            Response response = builder.build().newCall(new WrappedRequest().Get("http://data.alexa.com/data?cli=10&url="+URL)).execute();
            String xml = response.body().string();
            Document doc = Jsoup.parse(xml, "", Parser.xmlParser());
            Elements popularity = doc.select("POPULARITY");
            if (!popularity.isEmpty()) {
                for (Element e : popularity) {
                    ranking = e.attr("TEXT");
                }
            } else {
                ranking = "10000000";
            }
        } catch (IOException e) {
            logger.error("Alexa ranking lookup failed", e);
        }
        return ranking;
    }
    
    private Proxy buildProxy() throws UnknownHostException {
        int nextPick = new Random().nextInt(proxyList.size());
        String nextProxy = proxyList.get(nextPick);
        String[] hostAndPort = nextProxy.split(":");
        String host = hostAndPort[0];
        int port = Integer.parseInt(hostAndPort[1]);
        return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(InetAddress.getByName(host), port));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "ranking"));
    }

}
