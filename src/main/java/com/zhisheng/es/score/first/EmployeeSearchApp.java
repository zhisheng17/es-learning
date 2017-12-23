package com.zhisheng.es.score.first;

import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

/**
 * 员工搜索应用程序
 *
 * Created by zhisheng_tian on 2017/12/22.
 */
public class EmployeeSearchApp {
    @SuppressWarnings({"unchecked", "resource"})
    public static void main(String[] args) throws Exception {
        // 先构建client
        Settings settings = Settings.builder()
                .put("cluster.name", "zhisheng")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.153.135"), 9300));

//        prepareData(client);

//        executeSearch(client);
//        search(client);
//        getSearch(client);
//        multiGet(client);

//        bulk(client);


        bulkProcessor(client);
        client.close();
    }

    /**
     * 查询
     *
     * @param client
     * @throws Exception
     */
    private static void executeSearch(TransportClient client) throws Exception {
        SearchResponse response = client.prepareSearch("company")
                .setTypes("employee")
                .setQuery(QueryBuilders.matchQuery("position", "technique"))
                .setPostFilter(QueryBuilders.rangeQuery("age").from(30).to(40))     //年龄范围
                .setFrom(0).setSize(2)      //from：从哪开始，size：查询信息条数
                .get();
        SearchHit[] hits = response.getHits().getHits();
        for (int i = 0; i < hits.length; i++) {
            System.out.println(hits[i].getSourceAsString());
        }
    }


    /**
     * 查询索引的信息
     *
     * @param client
     */
    private static void getSearch(TransportClient client) {
        GetResponse response = client.prepareGet("company", "employee", "1").get();
        System.out.println(response.getId());
        System.out.println(response.getSource());
        System.out.println(response.getIndex());
        System.out.println(response.getType());
        System.out.println(response.getVersion());
        System.out.println(response.getSourceAsString());
        //fields
        response.getSourceAsMap().forEach((k, v) -> System.out.println(k + " : " + v));
    }


    /**
     * multi get
     *
     * @param client
     */
    private static void multiGet(TransportClient client) {
        MultiGetResponse responses = client.prepareMultiGet()
                .add("company", "employee", "1")
                .add("company", "employee", "2")
                .add("company", "employee", "3")
                .add("company", "employee", "4")
                .add("website", "blog", "2")
                .add("website", "blog", "4")
                .get();
        responses.forEach(r -> {
            System.out.println(r.getId());
            System.out.println(r.getIndex());
            System.out.println(r.getType());
            System.out.println("-----------------");
        });
    }


    /**
     * bulk
     *
     * @param client
     * @throws IOException
     */
    private static void bulk(TransportClient client) throws IOException {
        BulkRequestBuilder bulk = client.prepareBulk();
        bulk.add(client.prepareIndex("company", "employee", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "jack")
                        .field("age", 27)
                        .field("position", "technique software")
                        .field("country", "china")
                        .field("join_date", "2017-01-01")
                        .field("salary", 10000)
                        .endObject())
                );
        bulk.add(
                client.prepareIndex("company", "employee", "2")
                        .setSource(XContentFactory.jsonBuilder()
                                .startObject()
                                .field("name", "marry")
                                .field("age", 35)
                                .field("position", "technique manager")
                                .field("country", "china")
                                .field("join_date", "2017-01-02")
                                .field("salary", 12000)
                                .endObject())
                );
        System.out.println(bulk.get().getTook());
    }


    /**
     * 参考 ： https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-bulk-processor.html
     *
     * bulk Processor
     *
     * @param client
     * @throws InterruptedException
     */
    private static void bulkProcessor(TransportClient client) throws Exception {
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                System.out.println("------before----");
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                System.out.println("------after------");
            }

            // 执行出错时执行
            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                System.out.println("-----Throwable----");
            }

        }).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))           // 1gb的数据刷新一次bulk
                .setBulkActions(10000)                                      //1w次请求执行一次bulk
                .setFlushInterval(TimeValue.timeValueSeconds(5))            // 固定5s必须刷新一次
                .setConcurrentRequests(1)                                   // 并发请求数量, 0不并发, 1并发允许执行
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))  // 设置退避, 100ms后执行, 最大请求3次
                .build();

        // 添加单次请求
        bulkProcessor.add(new IndexRequest("company", "employee", "1"));
        bulkProcessor.add(new IndexRequest("company", "employee", "2"));

        bulkProcessor.flush();

        // 关闭
        bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
        // 或者
        bulkProcessor.close();

        client.admin().indices().prepareRefresh().get();

//        SearchResponse response = client.prepareSearch().get();
        executeSearch(client);
    }

    /**
     * 查询单条索引的全部信息
     *
     * @param client
     * @throws IOException
     */
    private static void search(TransportClient client) throws IOException {
        IndexResponse response = client.prepareIndex("company", "employee", "6")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "he")
                        .field("age", 41)
                        .field("position", "finance manager")
                        .field("country", "usa")
                        .field("join_date", "2015-01-06")
                        .field("salary", 15000)
                        .endObject())
                .get();

        System.out.println(response.getIndex());    //company
        System.out.println(response.getResult());   //CREATED
        System.out.println(response.getType());     //employee
        System.out.println(response.getVersion());  //1
        System.out.println(response.getShardId());  //[company][2]
        System.out.println(response.getShardInfo());    //ShardInfo{total=2, successful=2, failures=[]}
        System.out.println(response.getId());       //6
    }

    /**
     * 准备数据
     *
     * @param client
     * @throws Exception
     */
    private static void prepareData(TransportClient client) throws Exception {
        client.prepareIndex("company", "employee", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "jack")
                        .field("age", 27)
                        .field("position", "technique software")
                        .field("country", "china")
                        .field("join_date", "2017-01-01")
                        .field("salary", 10000)
                        .endObject())
                .get();

        client.prepareIndex("company", "employee", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "marry")
                        .field("age", 35)
                        .field("position", "technique manager")
                        .field("country", "china")
                        .field("join_date", "2017-01-02")
                        .field("salary", 12000)
                        .endObject())
                .get();

        client.prepareIndex("company", "employee", "3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "tom")
                        .field("age", 32)
                        .field("position", "senior technique software")
                        .field("country", "china")
                        .field("join_date", "2016-01-03")
                        .field("salary", 11000)
                        .endObject())
                .get();

        client.prepareIndex("company", "employee", "4")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "jen")
                        .field("age", 25)
                        .field("position", "junior finance")
                        .field("country", "usa")
                        .field("join_date", "2016-01-04")
                        .field("salary", 7000)
                        .endObject())
                .get();

        client.prepareIndex("company", "employee", "5")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "mike")
                        .field("age", 37)
                        .field("position", "finance manager")
                        .field("country", "usa")
                        .field("join_date", "2015-01-05")
                        .field("salary", 15000)
                        .endObject())
                .get();

    }
}
