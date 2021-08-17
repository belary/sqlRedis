package com.mgtv.data.biz;

public class StaticSql {

    public static String getLiveEventTableSql() {
        return "CREATE VIEW live_event  AS\n" +
                "SELECT * FROM hive_event_live\n" +
                "WHERE $page_name = 'live_room' \n" +
                "  AND live_id IS NOT NULL\n" +
                "  AND show_id IS NOT NULL\n" +
                "  AND $id IS NOT NULL\n" +
                "  AND $event_name in ('show', 'stay', 'page', 'click')";
    }

    public static String getCollectSql() {
        return "INSERT INTO statistics_per_liveroom\n" +
                "SELECT * FROM (\n" +
                "    (SELECT\n" +
                "         live_id as live_id,\n" +
                "         show_id as show_id,\n" +
                "         'subtarget' as statics_type,\n" +
                "         sub_target_type as sub_target_type,\n" +
                "         sub_target_id as sub_target_id, \n" +
                "         action_type as action_type,\n" +
                "         0 as sum_duration,\n" +
                "         pv as pv,\n" +
                "         uv as uv\n" +
                "    FROM live_sub_target_s) \n" +
                "    UNION ALL \n" +
                "    (SELECT\n" +
                "      live_id as live_id,\n" +
                "      show_id as show_id,\n" +
                "      'stay' as statics_type,\n" +
                "      '' as sub_target_type,\n" +
                "      '' as sub_target_id, \n" +
                "      '' as action_type,\n" +
                "      sum_duration as sum_duration,\n" +
                "      pv as pv,\n" +
                "      uv as uv\n" +
                "    FROM live_duration)\n" +
                "    UNION ALL \n" +
                "    (SELECT\n" +
                "      live_id as live_id,\n" +
                "      show_id as show_id,\n" +
                "      'root' as statics_type,\n" +
                "      '' as sub_target_type,\n" +
                "      '' as sub_target_id, \n" +
                "      '' as action_type,\n" +
                "      0 as sum_duration,\n" +
                "      pv as pv,\n" +
                "      uv as uv \n" +
                "    FROM live_s)" +
                ")\n";
    }

    public static String getClickShowTable() {
        return "CREATE VIEW live_sub_target_s AS\n" +
                "SELECT live_id, show_id,\n" +
                "       COUNT(1) as pv, COUNT(DISTINCT $device_id) as uv,\n" +
                "       sub_target_type, sub_target_id, action_type\n" +
                "FROM live_sub_target\n" +
                "WHERE action_type in ('show', 'click')\n" +
                "GROUP BY live_id, show_id,sub_target_type, sub_target_id, action_type";
    }

    public static String getClickShowDidTable() {
        return "CREATE VIEW live_sub_target AS\n" +
                "SELECT live_id, show_id, \n" +
                "       IF($element_id = 'click_live_praise', 'live_praise', IF(mod_id = 'all_goods', 'goods', mod_id)) as sub_target_type,\n" +
                "       IF(mod_id in ('goods', 'all_goods'), goods_id, '') as sub_target_id,\n" +
                "       IF($element_id like 'click_%', 'click', IF($event_name='show', 'show', '')) as action_type,\n" +
                "       $device_id, \n" +
                "       mod_id, \n" +
                "       $element_id, \n" +
                "       goods_id, \n" +
                "       $event_name \n" +
                "FROM live_event\n" +
                "WHERE mod_id in ('goods', 'all_goods', 'redpack', 'answer', 'pendant', 'prize_backpack') OR $element_id = 'click_live_praise'";
    }

    public static String getPageTableSql() {
        return "CREATE VIEW live_s AS\n" +
                "SELECT \n" +
                "    live_id, show_id,\n" +
                "    COUNT(1) as pv,\n" +
                "    COUNT(DISTINCT $device_id) as uv \n" +
                " FROM \n" +
                "live_event\n" +
                "WHERE $event_name = 'page'\n" +
                "GROUP BY live_id, show_id";
    }

    public static String genSourceTableSql(String bootStrapServers, String topic, String groupId) {
        return "CREATE TABLE hive_event_live (\n" +
                "    $id STRING,\n" +
                "    $page_name STRING,\n" +
                "    live_id STRING, \n" +
                "    show_id STRING, \n" +
                "    biz_type STRING, \n" +
                "    mod_id STRING, \n" +
                "    $element_id STRING, \n" +
                "    goods_id STRING,\n" +
                "    $device_id STRING,\n" +
                "    $event_name STRING, \n" +
                "    event_duration int, \n" +
                "    $event_time STRING \n" +
                ")  WITH (\n" +
                "          'connector' = 'kafka',\n" +
                String.format("          'topic' = '%s',\n", topic) +
                String.format("          'properties.bootstrap.servers' = '%s',\n", bootStrapServers) +
                String.format("          'properties.group.id' = '%s',\n", groupId) +
                "          'scan.startup.mode' = 'group-offsets',\n" +
                "          'format' = 'json'\n" +
                "        )";
    }

    public static String genSinkTableSql(String sinkBootstrapServers, String topic) {
        return "create table statistics_per_liveroom (\n" +
                "        live_id STRING,\n" +
                "        show_id STRING,\n" +
                "        statics_type STRING, \n" +
                "        sub_target_type STRING,\n" +
                "        sub_target_id STRING, \n" +
                "        action_type STRING, \n" +
                "        sum_duration BIGINT, \n" +
                "        pv BIGINT,\n" +
                "        uv BIGINT,\n" +
                "        PRIMARY KEY (live_id) NOT ENFORCED\n" +
                "    ) with (\n" +
                "          'connector' = 'upsert-kafka',\n" +
                String.format("          'topic' = '%s',\n", topic) +
                String.format("          'properties.bootstrap.servers' = '%s',\n", sinkBootstrapServers) +
                "          'key.format' = 'json',\n" +
                "          'value.format' = 'json'\n" +
                "    )";
    }

    public static String genStayTableSql(){
        return "CREATE VIEW live_duration AS\n" +
                "SELECT \n" +
                "    live_id, show_id,\n" +
                "    SUM(event_duration / 1000) as sum_duration,\n" +
                "    COUNT(1) as pv,\n" +
                "    COUNT(DISTINCT $device_id) as uv \n" +
                " FROM \n" +
                "live_event \n" +
                "GROUP BY live_id, show_id";
    }
}
