package fogetti.phish.storm.relatedness.intersection;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

/**
 * Makes a request to the Alexa Web Information Service UrlInfo action.
 */
public class UrlInfo {

    private static final String ACTION_NAME = "UrlInfo";
    private static final String RESPONSE_GROUP_NAME = "Rank";
    private static final String SERVICE_HOST = "awis.amazonaws.com";
    private static final String UTF8_CHARSET = "UTF-8";
    private static final String HMAC_SHA256_ALGORITHM = "HmacSHA256";
    private static final String REQUEST_METHOD = "GET";

    private final String awsAccessKeyId;
    private final String awsSecretKey;
    private String site;

    private final SecretKeySpec secretKeySpec;
    private final Mac mac;
    private final Encoder encoder = Base64.getEncoder();

    public UrlInfo(String accessKey, String secretKey, String site) throws IOException, NoSuchAlgorithmException, InvalidKeyException {
        this.awsAccessKeyId = accessKey;
        this.awsSecretKey = secretKey;
        this.site = site;
        byte[] secretyKeyBytes = awsSecretKey.getBytes(UTF8_CHARSET);
        this.secretKeySpec = new SecretKeySpec(secretyKeyBytes, HMAC_SHA256_ALGORITHM);
        this.mac = Mac.getInstance(HMAC_SHA256_ALGORITHM);
        this.mac.init(secretKeySpec);
    }

    public String sign(Map<String, String> params) {
        params.put("AWSAccessKeyId", awsAccessKeyId);
        params.put("Action", ACTION_NAME);
        params.put("ResponseGroup", RESPONSE_GROUP_NAME);
        params.put("SignatureMethod", HMAC_SHA256_ALGORITHM);
        params.put("SignatureVersion", "2");
        params.put("Timestamp", timestamp());
        params.put("Url", site);

        SortedMap<String, String> sortedParamMap =
                new TreeMap<String, String>(params);
        String canonicalQS = canonicalize(sortedParamMap);
        String toSign =
                REQUEST_METHOD + "\n"
                        + SERVICE_HOST + "\n"
                        + "/" + "\n"
                        + canonicalQS;

        String hmac = hmac(toSign);
        String sig = percentEncodeRfc3986(hmac);
        String url = "http://" + SERVICE_HOST + "?" +
                canonicalQS + "&Signature=" + sig;

        return url;
    }

    private String hmac(String stringToSign) {
        String signature = null;
        byte[] data;
        byte[] rawHmac;
        try {
            data = stringToSign.getBytes(UTF8_CHARSET);
            rawHmac = mac.doFinal(data);
            signature = encoder.encodeToString(rawHmac);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(UTF8_CHARSET + " is unsupported!", e);
        }
        return signature;
    }

    private String timestamp() {
        String timestamp = null;
        Calendar cal = Calendar.getInstance();
        DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        dfm.setTimeZone(TimeZone.getTimeZone("GMT"));
        timestamp = dfm.format(cal.getTime());
        return timestamp;
    }

    private String canonicalize(SortedMap<String, String> sortedParamMap)
    {
        if (sortedParamMap.isEmpty()) {
            return "";
        }

        StringBuffer buffer = new StringBuffer();
        Iterator<Map.Entry<String, String>> iter =
                sortedParamMap.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<String, String> kvpair = iter.next();
            buffer.append(percentEncodeRfc3986(kvpair.getKey()));
            buffer.append("=");
            buffer.append(percentEncodeRfc3986(kvpair.getValue()));
            if (iter.hasNext()) {
                buffer.append("&");
            }
        }
        String canonical = buffer.toString();
        return canonical;
    }

    private String percentEncodeRfc3986(String s) {
        String out;
        try {
            out = URLEncoder.encode(s, UTF8_CHARSET)
                    .replace("+", "%20")
                    .replace("*", "%2A")
                    .replace("%7E", "~");
        } catch (UnsupportedEncodingException e) {
            out = s;
        }
        return out;
    }

    /**
     * Makes a request to the specified Url and return the results as a String
     *
     * @param requestUrl url to make request to
     * @return the XML document as a String
     * @throws IOException
     */
    public String makeRequest(String requestUrl) throws IOException {
        URL url = new URL(requestUrl);
        URLConnection conn = url.openConnection();
        InputStream in = conn.getInputStream();

        // Read the response
        StringBuffer sb = new StringBuffer();
        int c;
        int lastChar = 0;
        while ((c = in.read()) != -1) {
            if (c == '<' && (lastChar == '>'))
                sb.append('\n');
            sb.append((char) c);
            lastChar = c;
        }
        in.close();
        return sb.toString();
    }

    /**
     * Makes a request to the Alexa Web Information Service UrlInfo action
     */
    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: UrlInfo ACCESS_KEY_ID " +
                    "SECRET_ACCESS_KEY site");
            System.exit(-1);
        }

        // Read command line parameters

        String accessKey = args[0];
        String secretKey = args[1];
        String site = args[2];

        UrlInfo urlInfo = new UrlInfo(accessKey, secretKey, site);

        String uri = urlInfo.sign(new HashMap<>());
        System.out.println("Making request to:\n");
        System.out.println(uri + "\n");

        // Make the Request

        String xmlResponse = urlInfo.makeRequest(uri);

        // Print out the XML Response

        System.out.println("Response:\n");
        System.out.println(xmlResponse);
        Document doc = Jsoup.parse(xmlResponse, "", Parser.xmlParser());
        Elements rank = doc.select("aws|Rank");
        if (!rank.isEmpty()) {
            System.out.println(String.format("Ranking is [%s]", rank.text()));
        }
    }
}
