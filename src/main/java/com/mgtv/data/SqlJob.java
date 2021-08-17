/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mgtv.data;

import com.mgtv.data.utils.MyParameterParser;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

import static com.mgtv.data.biz.StaticSql.*;
import static com.mgtv.data.biz.StaticSql.getCollectSql;


public class SqlJob {

	public static void main( String[] args ) throws Exception {

		// 参数解析 run_at in (dev, prd ) =>
		// dev => config.dev.properties
		ParameterTool cliAgs = ParameterTool.fromArgs(args);
		String runningAt = cliAgs.get("run_at");
		ParameterTool parameter = new MyParameterParser().getParameter(runningAt);
		if (parameter == null) {
			System.out.println("未找到配置文件");
			return;
		}

		// 应用配置参数到执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		initConfig(env, tableEnv, parameter);


		// 业务
		String inputBootStrapServer = parameter.get("INPUT_BROKERS");
		String outputBrokers = parameter.get("OUTPUT_BROKERS");
		String inputTopic = parameter.get("INPUT_TOPIC");
		String outputKey=parameter.get("OUTPUT_UV_KEY");
		String consumerInputGroupId = parameter.get("INPUT_GROUP_ID");

		// input table
		String sourceTableSql = genSourceTableSql(inputBootStrapServer, inputTopic, consumerInputGroupId);
		tableEnv.executeSql(sourceTableSql);

		// output table
		String outputTableSql = genSinkTableSql(outputBrokers, outputTopic);
		tableEnv.executeSql(outputTableSql);

		// pre-process live_event
		String liveEventTable = getLiveEventTableSql();
		tableEnv.executeSql(liveEventTable);

		// duration
		String durationTable = genStayTableSql();
		tableEnv.executeSql(durationTable);

		// page
		String pageTable  = getPageTableSql();
		tableEnv.executeSql(pageTable);

		// click, show did
		String clickShowDidTable = getClickShowDidTable();
		tableEnv.executeSql(clickShowDidTable);

		// click show agg table
		String clickShowTable = getClickShowTable();
		tableEnv.executeSql(clickShowTable);

		// collect transform result to output table
		String collectSql = getCollectSql();
		tableEnv.executeSql(collectSql);
	}

	private static void initConfig(StreamExecutionEnvironment env,
								   StreamTableEnvironment tableEnvironment,
								   ParameterTool parameter) throws IOException {

		/*  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *
		 *  Stream Environment Setup
		 *  *  *  *  *  *  *  *  *  *  *  *  *  *  * *  *  *  *  *  *  *  *  *  *  *  *  *  * */
		//设置重启策略防止程序错误导致OOM
		env.setRestartStrategy(
				RestartStrategies.fixedDelayRestart(1, 3000)
		);

		//为了保证数据一致性,开启checkpoint
		env.enableCheckpointing(60 * 1000);

		// 设置最大并行到checkpoint
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();

		// 同一时间只允许进行一个检查点
		checkpointConfig.setMaxConcurrentCheckpoints(1);

		// 设置checkpoint的超时时间
		checkpointConfig.setCheckpointTimeout(1000 * 60 * 5);

		// 确保检查点之间有至少10s的间隔【checkpoint最小间隔】
		checkpointConfig.setMinPauseBetweenCheckpoints(10 * 1000);
		checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		// 被cancel后，会保留Checkpoint数据
		checkpointConfig.enableExternalizedCheckpoints(
				CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		//使用rocksdb作为状态保存
		String strRocksDb = parameter.get("ROCKS_DB_ADDR");
		StateBackend stateBackend =
				new RocksDBStateBackend(strRocksDb);
		env.setStateBackend(stateBackend);


		/*  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *
		 *  Stream Table Environment Setup
		 *  *  *  *  *  *  *  *  *  *  *  *  *  *  * *  *  *  *  *  *  *  *  *  *  *  *  *  * */
		Configuration configuration = tableEnvironment.getConfig().getConfiguration();
		configuration.setString("table.exec.mini-batch.enabled", "true");
		configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
		configuration.setString("table.exec.mini-batch.size", "50000");
		configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
		configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");
		configuration.setString("table.exec.sink.not-null-enforcer", "drop");

	}
}
