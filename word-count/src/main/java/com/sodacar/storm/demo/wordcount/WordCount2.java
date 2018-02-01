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
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordCount2 {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {
        LocalCluster cluster = new LocalCluster();

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout.setCycle(true);

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("127.0.0.1").setPort(46379)
                .build();

        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisLookupMapper lookupMapper = new WordCountLookupMapper();
        RedisState.Factory factory = new RedisState.Factory(poolConfig);

        TridentTopology topology = new TridentTopology();

        Stream newStream = topology.newStream("spout1", spout)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .newValuesStream();

        Fields inputFields = new Fields("word", "count");

//        newStream.each(inputFields, new PrintFunction(), new Fields());

//
        newStream
                .partitionPersist(factory,
                        inputFields,
                        new RedisStateUpdater(storeMapper).withExpire(86400000),
                        new Fields());
//
////                .persistentAggregate(factory, new Count(), new RedisStateUpdater(storeMapper).withExpire(86400000), new Fields("count"))
////                .parallelismHint(6);
//
////        stream.partitionPersist(factory,
////                new Fields("word", "count"),
////                new RedisStateUpdater(storeMapper).withExpire(86400000),
////                new Fields());
//

        TridentState state = topology.newStaticState(factory);
        Stream stream = newStream.stateQuery(state, new Fields("word"),
                new RedisStateQuerier(lookupMapper),
                new Fields("columnName", "columnValue"));
        stream.each(new Fields("word", "columnValue"), new PrintFunction(), new Fields());

        topology.build();

        cluster.submitTopology("wordCounter", new Config(), topology.build());

//        cluster.shutdown();
    }
}
