package storm.cookbook.tfidf.functions;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Time;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * User: domenicosolazzo
 */
public class PersistDocumentFunction extends BaseFunction {


    Logger LOG = LoggerFactory.getLogger(PersistDocumentFunction.class);
    private static final long serialVersionUID = 1L;

    DataFileWriter<GenericRecord> dataFileWriter;
    Schema schema;



    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        try {
            String path = (String) conf.get("DOCUMENT_PATH");
            schema = Schema.parse(PersistDocumentFunction.class
                    .getResourceAsStream("/document.avsc"));
            File file = new File(path);
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
            dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
            if(file.exists())
                dataFileWriter.appendTo(file);
            else
                dataFileWriter.create(schema, file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void cleanup() {
        try {
            dataFileWriter.close();
        } catch (IOException e) {
            LOG.error("Error Closing file: " + e);
        }
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        GenericRecord docEntry = new GenericData.Record(schema);
        docEntry.put("docid", tuple.getStringByField("documentId"));
        docEntry.put("time", Time.currentTimeMillis());
        docEntry.put("line", tuple.getStringByField("document"));
        try {
            dataFileWriter.append(docEntry);
            dataFileWriter.flush();
        } catch (IOException e) {
            LOG.error("Error writing to document record: " + e);
            throw new RuntimeException(e);
        }

    }

}
