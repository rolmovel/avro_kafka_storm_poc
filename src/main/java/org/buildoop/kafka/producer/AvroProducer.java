package org.buildoop.kafka.producer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.schemarepo.SchemaEntry;
import org.schemarepo.Subject;
import org.schemarepo.SubjectConfig;
import org.schemarepo.client.RESTRepositoryClient;
import org.schemarepo.json.GsonJsonUtil;

import kafka.message.Message;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class AvroProducer {

	
	private final static String esquema = "{" + 
			"  \"type\": \"record\"," + 
			"  \"name\": \"LPEvent\"," + 
			"  \"doc\": \"LivePErson event\"," + 
			"  \"fields\": [" + 
			"    {\"name\": \"revision\", \"type\": \"long\"}," + 
			"    {\"name\": \"siteId\", \"type\": \"string\"}," + 
			"    {\"name\": \"eventType\", \"type\": \"string\"}," + 
			"    {\"name\": \"timeStamp\", \"type\": \"long\"}," + 
			"    {\"name\": \"sessionId\", \"type\": \"string\"}," + 
			"    {\"name\": \"subrecord\"," + 
			"            \"type\":[" + 
			"            {" + 
			"            \"name\": \"pline\"," + 
			"            \"type\": \"record\"," + 
			"             \"fields\": [   {\"name\": \"text\", \"type\": \"string\"}," + 
			"                           {\"name\": \"lineType\", \"type\": \"int\"}," + 
			"                           {\"name\": \"repId\", \"type\": \"string\"}" + 
			"                       ]" + 
			"             }," + 
			"             {" + 
			"             \"name\": \"impressionDisplay\"," + 
			"             \"type\": \"record\"," + 
			"              \"fields\": [   {\"name\": \"channel\", \"type\": \"int\"}," + 
			"                            {\"name\": \"impressionState\", \"type\": \"int\"}," + 
			"                            {\"name\": \"skillId\", \"type\": \"long\"}," + 
			"                            {\"name\": \"impDisplayObjId\", \"type\": \"long\"}" + 
			"                        ]" + 
			"              }," + 
			"              {" + 
			"             \"name\": \"impressionResponse\"," + 
			"             \"type\": \"record\"," + 
			"              \"fields\": [   {\"name\": \"channel\", \"type\": \"int\"}," + 
			"                            {\"name\": \"impressionState\", \"type\": \"int\"}," + 
			"                            {\"name\": \"skillId\", \"type\": \"long\"}," + 
			"                            {\"name\": \"agentId\", \"type\": \"long\"}," + 
			"                            {\"name\": \"impDisplayObjId\", \"type\": \"long\"}," + 
			"                            {\"name\": \"impResponseId\", \"type\": \"long\"}" + 
			"                        ]" + 
			"              }," + 
			"              \"null\"" + 
			"          ]}" + 
			"      ]" + 
			"}";
	
	
    //    public final String zkConnection = "tlvwhale1:2181,tlvwhale2:2181,tlvwhale3:2181";
    public final static String zkConnection = "master.rolmo:2181";
    public final static String brokerList = "slave1.rolmo:9092, slave2.rolmo:9092";
    public final static String topic = "avrotopic";

    public static void main(String args[]){
    	try{

        RESTRepositoryClient rescCl = new RESTRepositoryClient("http://localhost:2876/schema-repo/", new GsonJsonUtil(), false);

        SchemaEntry sss = rescCl.lookup("keedio").lookupById("0");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(sss.getSchema());
        
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("revision", 1L);
        datum.put("siteId", "28280110");
        datum.put("eventType", "PLine");
        datum.put("timeStamp", System.currentTimeMillis());
        datum.put("sessionId", "123456II");

        Map<String, Schema> unions = new HashMap<String, Schema>();
        List<Schema> typeList = schema.getField("subrecord").schema().getTypes();
        for (Schema sch : typeList){
            unions.put(sch.getName(), sch);
        }

        GenericRecord plineDatum = new GenericData.Record(unions.get("pline"));
        plineDatum.put("text", "How can I help you?");
        plineDatum.put("lineType", 1);
        plineDatum.put("repId", "REPID12345");

        datum.put("subrecord", plineDatum );


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();
        Message message = new Message(out.toByteArray());


        Properties props = new Properties();
        props.put("zk.connect", zkConnection);
        props.put("metadata.broker.list", brokerList);
        Producer<String, byte[]> producer = new Producer<String, byte[]>(new ProducerConfig(props));
        producer.send(new KeyedMessage(topic, out.toByteArray())); 
        
    	} catch (Exception e) {
    		e.printStackTrace();
    	} catch (Throwable e) {
    		e.printStackTrace();
    	}
    }


}