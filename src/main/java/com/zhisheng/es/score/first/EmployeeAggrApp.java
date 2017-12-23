package com.zhisheng.es.score.first;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Map;

/**
 * 员工聚合分析应用程序
 *
 * Created by zhisheng_tian on 2017/12/22.
 */
public class EmployeeAggrApp {
    @SuppressWarnings({"unchecked", "resource"})
    public static void main(String[] args) throws Exception {
        // 先构建client
        Settings settings = Settings.builder()
                .put("cluster.name", "zhisheng")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.153.135"), 9300));


        agg(client);

        client.close();
    }


    private static void agg(TransportClient client) {
        SearchResponse searchResponse = client.prepareSearch("company")
                .addAggregation(AggregationBuilders.terms("group_by_country").field("country")
                        .subAggregation(AggregationBuilders.dateHistogram("group_by_join_date")
                                .field("join_date")
                                .dateHistogramInterval(DateHistogramInterval.YEAR)
                                .subAggregation(AggregationBuilders.avg("avg_salary").field("salary")))
                ).execute().actionGet();

        Map<String, Aggregation> map = searchResponse.getAggregations().asMap();
        StringTerms groupByCountry = (StringTerms) map.get("group_by_country");
        groupByCountry.getBuckets().forEach(b -> {
            System.out.println(b.getKey() + " : " + b.getDocCount());
            Histogram groupByJoinDate = (Histogram) b.getAggregations().asMap().get("group_by_join_date");
            groupByJoinDate.getBuckets().forEach(buc -> {
                System.out.println(buc.getKey() + " : " + buc.getDocCount());
                Avg avg = (Avg) buc.getAggregations().asMap().get("avg_salary");
                System.out.println(avg.getValue());
            });
        });

    }
}
