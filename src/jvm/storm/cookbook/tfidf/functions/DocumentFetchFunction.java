package storm.cookbook.tfidf.functions;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import twitter4j.URLEntity;
import au.com.bytecode.opencsv.CSVReader;
import backtype.storm.Config;
import backtype.storm.tuple.Values;


/**
 * User: domenicosolazzo
 */
public class DocumentFetchFunction extends BaseFunction {
    private static final long serialVersionUID = 1L;

    private List<String> mimeTypes;
    private Map<String, String> testData = new HashMap<String, String>();
    private Boolean testMode = false;

    private Logger LOG = LoggerFactory.getLogger(DocumentFetchFunction.class);
    @Override
    public void prepare(Map conf, TridentOperationContext context){
        if (conf.get(backtype.storm.Config.TOPOLOGY_DEBUG).equals(true)) {
            testMode = true;
            CSVReader reader;
            try {
                reader = new CSVReader(new BufferedReader(
                        new InputStreamReader(
                                DocumentFetchFunction.class
                                        .getResourceAsStream("docs.csv"))));
                List<String[]> myEntries = reader.readAll();
                for(String[] row : myEntries){
                    testData.put(row[0], row[1]);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String url = tuple.getStringByField("url");
        if (testMode) {
            LOG.debug("Generating fake document for testing");
            String contents = testData.get(url);
            if(contents != null)
                collector.emit(new Values(contents, url.trim(),"twitter"));
        } else {
            try {
                Parser parser = new AutoDetectParser();
                Metadata metadata = new Metadata();
                ParseContext parseContext = new ParseContext();
                URL urlObject = new URL(url);
                ContentHandler handler = new BodyContentHandler(10 * 1024 * 1024);
                parser.parse((InputStream) urlObject.getContent(), handler,
                        metadata, parseContext);
                String[] mimeDetails = metadata.get("Content-Type").split(";");
                if ((mimeDetails.length > 0)
                        && (mimeTypes.contains(mimeDetails[0]))) {
                    collector.emit(new Values(handler.toString(), url.trim(),
                            "twitter"));
                }
            } catch (Exception e) {
            }
        }
    }
}
