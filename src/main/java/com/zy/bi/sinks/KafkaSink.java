package com.zy.bi.sinks;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.huaweicloud.dis.DISAsync;
import com.huaweicloud.dis.DISClientAsyncBuilder;
import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.core.handler.AsyncHandler;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import com.huaweicloud.dis.util.JsonUtils;
import com.mongodb.DBObject;
import com.zy.bi.core.MongoShardsTask;
import com.zy.bi.core.MongoTask;
import com.zy.bi.util.Common;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by allen on 2015/8/28.
 */
public class KafkaSink extends AbstractSink {

    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);

    private static final int MB = 1*1024*1024;

    private DISConfig disConfig = new DISConfig();
    private DISAsync disClientAsync;
    private String streamName;
    private int maxMsgSize;
    private String userAndPassword;

    // Kafka config
//    private Properties kafkaProps;
//    private Producer<String, byte[]> producer;
//    private String topic;
//    private int batchSize;
//    private List<KeyedMessage<String, byte[]>> messageList;

    // Inner queue config
    private LinkedBlockingQueue<DBObject> queue;
    private int queueSize;

    // Mongo config
    private MongoTask mongoTask;
    private MongoShardsTask mongoShardsTask;
    private MongoStatus mongoStatus;
    private String mongoUri;
    private String[] filteredNs;
    private String[] ns;

    private ConcurrentHashMap<String,LinkedBlockingQueue<DBObject>> nsMap;
    private ConcurrentHashMap<String,AtomicBoolean> sending;

    // Keeps the running state
    private AtomicBoolean running = new AtomicBoolean(true);

    @Override
    public void process() {

        Thread t = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                sendToDis();
            }
        });
        t.setName("sending-to-dis");
        t.start();
        // While the thread is set to running
        long num = 0;
        long printNum = 0;
        while (running.get()) {
            if(System.currentTimeMillis() / 10000 > printNum)
            {
                printNum = System.currentTimeMillis() / 10000;
                LOGGER.info("read oplog size: " + num);
            }
            try {
                DBObject object = queue.poll();
                if (object == null)
                {
                    Thread.sleep(50);
                    continue;
                }
                Object op = object.get("op");
                if(op == null)
                {
                    continue;
                }
                String opStr = (String) op;
                if(opStr == null || opStr.isEmpty())
                {
                    continue;
                }
                if(!Common.in_range(opStr, "i", "u", "d"))
                {
                    continue;
                }

                Object ns = object.get("ns");
                if(ns == null)
                {
                    continue;
                }
                String nsStr = (String) ns;
                if(nsStr == null || nsStr.isEmpty())
                {
                    LOGGER.warn("opLog error {}", object.toString());
                    continue;
                }
                LinkedBlockingQueue<DBObject> objects = nsMap.get(nsStr);
                if(objects == null)
                {
                    nsMap.put(nsStr,new LinkedBlockingQueue<>(queueSize));
                }
                num++;
                nsMap.get(nsStr).put(object);
            } catch (Exception e) {
                if (running.get()) throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void configure(String configFile) {

        Properties conf = new Properties();
        try {
            conf.load(new FileInputStream(configFile));
        } catch (Exception e) {
            LOGGER.warn("config file not found!");
        }

        String endpoint = conf.getProperty(KafkaSinkConstants.ENDPOINT);
        if(StringUtils.isEmpty(endpoint))
        {
            LOGGER.error("endpoint is empty");
            System.exit(-1);
        }
        String region = conf.getProperty(KafkaSinkConstants.REGION);
        if(StringUtils.isEmpty(region))
        {
            LOGGER.error("region is empty");
            System.exit(-1);
        }
        String ak = conf.getProperty(KafkaSinkConstants.AK);
        if(StringUtils.isEmpty(ak))
        {
            LOGGER.error("ak is empty");
            System.exit(-1);
        }
        String sk = conf.getProperty(KafkaSinkConstants.SK);
        if(StringUtils.isEmpty(sk))
        {
            LOGGER.error("sk is empty");
            System.exit(-1);
        }
        String projectId = conf.getProperty(KafkaSinkConstants.PROJECT_ID);
        if(StringUtils.isEmpty(projectId))
        {
            LOGGER.error("projectId is empty");
            System.exit(-1);
        }
        String streamName = conf.getProperty(KafkaSinkConstants.STREAM_NAME);
        if(StringUtils.isEmpty(streamName))
        {
            LOGGER.error("streamName is empty");
            System.exit(-1);
        }
        disConfig.setAK(ak);
        disConfig.setSK(sk);
        disConfig.setEndpoint(endpoint);
        disConfig.setRegion(region);
        disConfig.setProjectId(projectId);
        this.disClientAsync = DISClientAsyncBuilder.standard()
            .withAk(ak).withSk(sk).withEndpoint(endpoint).withRegion(region).withProjectId(projectId).build();
        this.streamName = streamName;

        String msgSize = conf.getProperty(KafkaSinkConstants.MAX_MSG_SIZE);
        if(StringUtils.isEmpty(msgSize))
        {
            LOGGER.warn("max.msg.size set to " + MB);
            this.maxMsgSize = MB;
        }
        else
        {
            this.maxMsgSize = Integer.valueOf(msgSize);
        }

//        String batch = conf.getProperty(KafkaSinkConstants.BATCH_SIZE);
//        if (batch == null) {
//            batchSize = KafkaSinkConstants.DEFAULT_BATCH_SIZE;
//        } else {
//            batchSize = Ints.tryParse(batch);
//        }
//        messageList =
//                new ArrayList<KeyedMessage<String, byte[]>>(batchSize);
//        LOGGER.debug("Using batch size: {}", batchSize);
//        topic = conf.getProperty(KafkaSinkConstants.TOPIC,
//                KafkaSinkConstants.DEFAULT_TOPIC);
//        if (topic.equals(KafkaSinkConstants.DEFAULT_TOPIC)) {
//            LOGGER.warn("The Property 'topic' is not set. " +
//                    "Using the default topic name: " +
//                    KafkaSinkConstants.DEFAULT_TOPIC);
//        } else {
//            LOGGER.info("Using the static topic: " + topic +
//                    " this may be over-ridden by event headers");
//        }
//        kafkaProps = KafkaSinkUtil.getKafkaProperties(conf);
        String qSize = conf.getProperty(KafkaSinkConstants.QUEUE_SIZE);
        if (qSize == null) {
            queueSize = KafkaSinkConstants.DEFAULT_QUEUE_SIZE;
        } else {
            queueSize = Ints.tryParse(qSize);
        }
        LOGGER.debug("Using batch size: {}", queueSize);
        queue = new LinkedBlockingQueue<DBObject>(queueSize);
        nsMap = new ConcurrentHashMap<>();
        sending = new ConcurrentHashMap<>();

        mongoUri = conf.getProperty(KafkaSinkConstants.MONGO_URI);
        Preconditions.checkNotNull(mongoUri);
        if (!mongoUri.startsWith("mongodb")) {
            mongoUri = "mongodb://" + mongoUri;
        }
        String mongoMode = conf.getProperty(KafkaSinkConstants.MONGO_MODE);
        Preconditions.checkNotNull(mongoMode);
        Preconditions.checkState(Common.in_range(mongoMode, "shard", "master-slave"), "mongoMode should be shard/master-slave");
        if (StringUtils.equals(mongoMode, "shard")) {
            mongoStatus = MongoStatus.SHARD;
        } else {
            mongoStatus = MongoStatus.MASS;
        }
        String filteredNameSpaces = conf.getProperty(KafkaSinkConstants.MONGO_FILTERED_NS);
        if (StringUtils.isNotBlank(filteredNameSpaces)) {
            filteredNs = filteredNameSpaces.split(",");
        } else {
            filteredNs = KafkaSinkConstants.MONGO_FILTEREDNAMESPACE;
        }

        String ns = conf.getProperty(KafkaSinkConstants.MONGO_NS);
        if (StringUtils.isNotBlank(ns)) {
            this.ns = ns.split(",");
        } else {
            this.ns = KafkaSinkConstants.MONGO_FILTEREDNAMESPACE;
        }
    }

    @Override
    public synchronized void start() {
        // Init the producer
//        ProducerConfig config = new ProducerConfig(kafkaProps);
//        producer = new Producer<String, byte[]>(config);
//        this.disKafkaProducer = new DISKafkaProducer<String, String>(disConfig);

        // Init mongo task
        if (mongoStatus == MongoStatus.MASS) {
            mongoTask = new MongoTask(queue, mongoUri, KafkaSinkConstants.MONGO_DBNAME, KafkaSinkConstants.MONGO_COLLECTIONNAME, filteredNs,ns);
            Thread thread = new Thread(mongoTask);
            thread.start();
        } else {
            mongoShardsTask = new MongoShardsTask(queue, mongoUri, KafkaSinkConstants.MONGO_DBNAME, KafkaSinkConstants.MONGO_COLLECTIONNAME, filteredNs,ns);
            mongoShardsTask.start();
        }
        // Start to process message in queue
        process();
    }

    @Override
    public synchronized void stop() {
        if (mongoStatus == MongoStatus.MASS) {
            mongoTask.stopThread();
        } else {
            mongoShardsTask.stop();
        }
        running.set(false);
    }

    public static enum MongoStatus {
        SHARD, MASS // master-slave
    }

    private void sendToDis()
    {
        AtomicLong suc = new AtomicLong();
        AtomicLong err = new AtomicLong();
        long printNum = 0;
        while (running.get())
        {
            try
            {
                if(System.currentTimeMillis() / 10000 > printNum)
                {
                    printNum = System.currentTimeMillis() / 10000;
                    LOGGER.info("send to dis, success: " + suc.get()  + ", error: " + err.get());
                }
                boolean sendRecord = false;
                for(Map.Entry<String, LinkedBlockingQueue<DBObject>> entry: nsMap.entrySet())
                {
                    if(sending.get(entry.getKey()) == null || sending.get(entry.getKey()).get() == false)
                    {
                        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
                        putRecordsRequest.setStreamName(streamName);
                        List<PutRecordsRequestEntry> list = new ArrayList<>();
                        LinkedBlockingQueue<DBObject> objects = entry.getValue();
                        if(objects == null)
                        {
                            continue;
                        }
                        int len = 0;
                        while (true)
                        {
                            DBObject object = objects.peek();
                            if(object == null)
                            {
                                break;
                            }
                            String msg = object.toString();
                            ByteBuffer buffer = ByteBuffer.wrap(msg.getBytes());
                            if(buffer.array().length > maxMsgSize)
                            {
                                LOGGER.error("msg size is larger than " + maxMsgSize +", {}",msg) ;
                                System.exit(-2);
                            }
                            if(len + buffer.array().length >= maxMsgSize)
                            {
                                break;
                            }
                            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
                            putRecordsRequestEntry.setData(buffer);
                            putRecordsRequestEntry.setPartitionKey(entry.getKey());
                            len += buffer.array().length;
                            list.add(putRecordsRequestEntry);
                            objects.poll();
                        }
                        if(list.isEmpty())
                        {
                            continue;
                        }
                        putRecordsRequest.setRecords(list);
                        AtomicBoolean b = sending.get(entry.getKey());
                        if(b == null){
                            sending.put(entry.getKey(),new AtomicBoolean(true));
                        }
                        sending.get(entry.getKey()).set(true);
                        final long msgNum = list.size();
                        sendRecord = true;
                        disClientAsync.putRecordsAsync(putRecordsRequest, new AsyncHandler<PutRecordsResult>()
                        {
                            @Override
                            public void onError(Exception e)
                            {
                                err.getAndAdd(msgNum);
                                LOGGER.error(e.getMessage(),e) ;
                                sending.get(entry.getKey()).set(false);
                            }

                            @Override
                            public void onSuccess(PutRecordsResult putRecordsResult)
                            {
                                suc.getAndAdd(msgNum);
                                LOGGER.debug("success to send {}", JsonUtils.objToJson(putRecordsRequest)); ;
                                sending.get(entry.getKey()).set(false);
                            }
                        });
                    }
                }
                if(!sendRecord)
                {
                    Thread.sleep(50);
                }
            }
            catch (Exception e)
            {
                LOGGER.error(e.getMessage(),e);
            }
        }
    }
}
