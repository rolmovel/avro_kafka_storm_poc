package org.buildoop.kafka.consumer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.Utils;
import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.schemarepo.SchemaEntry;
import org.schemarepo.client.RESTRepositoryClient;
import org.schemarepo.json.GsonJsonUtil;

import storm.kafka.KafkaConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Created with IntelliJ IDEA.
 * User: rans
 * Date: 2/7/13
 * Time: 2:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class AvroTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        //Add those lines to prevent too much logging noise in the console
        Logger.getLogger("storm.kafka.PartitionManager").setLevel(Level.ERROR);
        Logger.getLogger("backtype.storm").setLevel(Level.ERROR);
        Logger.getLogger("storm.kafka").setLevel(Level.ERROR);

        TopologyBuilder builder = new TopologyBuilder();
        int partitions = 1;
        final String offsetPath = "/liveperson-avro-test";
        final String consumerId = "v1";
        final String topic = "avrotopic";

        List<String> hosts = new ArrayList<String>();
        hosts.add("slave1.rolmo");
        hosts.add("slave2.rolmo");

        SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts("master.rolmo"), topic, offsetPath, consumerId);
        kafkaConfig.forceFromStart = true;
        
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

        builder.setSpout("spout", kafkaSpout);

        builder.setBolt("bolt", new AvroBolt()).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(true);

        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("avroTopology", conf, builder.createTopology());
            Utils.sleep(10000000);
            cluster.killTopology("avroTopology");
            cluster.shutdown();
        }

    }

    private static class AvroBolt extends BaseRichBolt {

        OutputCollector _collector;
        Schema _schema;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
            Schema.Parser parser = new Schema.Parser();
            Schema.Parser parser2 = new Schema.Parser();
            try {
                RESTRepositoryClient rescCl = new RESTRepositoryClient("http://localhost:2876/schema-repo/", new GsonJsonUtil(), false);

                SchemaEntry sss = rescCl.lookup("keedio").lookupById("0");

                _schema = parser.parse(sss.getSchema());

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void execute(Tuple tuple) {
            Message message = new Message((byte[])((TupleImpl) tuple).get("bytes"));

            ByteBuffer bb = message.payload();

            byte[] b = new byte[bb.remaining()];
            bb.get(b, 0, b.length);

            try {

                DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
                Decoder decoder = DecoderFactory.get().binaryDecoder(b, null);
                GenericRecord result = reader.read(null, decoder);
                System.out.println("siteId: "+ result.get("siteId"));
                System.out.println("eventType: "+ result.get("eventType"));
                Format formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                String s = formatter.format((Long) result.get("timeStamp"));
                System.out.println("timeStamp: " + s);
                System.out.println("Comment: " +result.get("comment") );

                System.out.println("PLine Text: " + ((GenericRecord) result.get("subrecord")).get("text"));
                System.out.println("PLine RepId: " + ((GenericRecord) result.get("subrecord")).get("repId"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }



            _collector.ack(tuple);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }


    }
}
