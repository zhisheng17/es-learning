package com.zhisheng.es.score.first;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * 查询集群的相关信息
 *
 * Created by zhisheng_tian on 2017/12/22.
 */
public class ElasticSearchInfo {
    @SuppressWarnings({"unchecked", "resource"})
    public static void main(String[] args) throws Exception {
        // 先构建client
        Settings settings = Settings.builder()
                .put("cluster.name", "zhisheng")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.153.135"), 9300));

        esInfo(client);

        client.close();
    }


    /**
     * 获取集群的相关信息
     *
     * @param client
     */
    private static void esInfo(TransportClient client) {
        client.connectedNodes().forEach(
                d -> {
                    System.out.println(d.getAddress());     //192.168.153.135:9300
                    System.out.println(d.getHostName());    //node1
                    System.out.println(d.getHostAddress()); //192.168.153.135
                    System.out.println(d.getVersion());     //5.5.2
                    System.out.println(d.getRoles());       //[MASTER, INGEST]
                    System.out.println(d.getName());        //node1
                }
        );
    }

}

