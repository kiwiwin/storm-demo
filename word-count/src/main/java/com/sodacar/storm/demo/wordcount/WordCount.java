package com.sodacar.storm.demo.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.redis.trident.state.RedisStateQuerier;
import org.apache.storm.redis.trident.state.RedisStateUpdater;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Random;

public class WordCount {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {
        LocalCluster cluster = new LocalCluster();

        Fields fields = new Fields("word", "count");
        Random random = new Random();
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                new Values("storm", random.nextInt()),
                new Values("trident", random.nextInt()),
                new Values("needs", random.nextInt()),
                new Values("javadoc", random.nextInt())
        );
        spout.setCycle(true);

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("127.0.0.1").setPort(46379)
                .build();

        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisLookupMapper lookupMapper = new WordCountLookupMapper();
        RedisState.Factory factory = new RedisState.Factory(poolConfig);



        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);


        stream.partitionPersist(factory,
                fields,
                new RedisStateUpdater(storeMapper).withExpire(86400000),
                new Fields());


        TridentState state = topology.newStaticState(factory);
        stream = stream.stateQuery(state, new Fields("word"),
                new RedisStateQuerier(lookupMapper),
                new Fields("columnName", "columnValue"));
        stream.each(new Fields("word", "columnValue"), new PrintFunction(), new Fields());
        topology.build();


        cluster.submitTopology("wordCounter", new Config(), topology.build());

//        cluster.shutdown();
    }
}
