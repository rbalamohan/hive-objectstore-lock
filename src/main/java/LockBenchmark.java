/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 * Intent is to benchmark acquireLocks time, by simulating a case
 * where could specify a table and number of partitions for which we need
 * to acquire the locks. Here we try to spin up multiple clients and each one
 * of them tries to acquire the locks from relevant table + partitions.
 * </pre>
 */
public class LockBenchmark {

  private static final MetricRegistry metrics = new MetricRegistry();

  //To indicate when we can start in parallel.
  private final CountDownLatch greenSignal;

  //For threading
  private final ExecutorService tp;
  private final int threads;
  private final int iterationsPerThread;

  //Table details for which we need to acquire locks
  private final String dbName;
  private final String tableName;
  private Table table;
  private List<Partition> partitionsList;
  private final int numPartitionsToFetch;

  public LockBenchmark(int threads, int iterationsPerThread, String dbName,
      String tableName, int numPartitionsToFetch) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.numPartitionsToFetch = numPartitionsToFetch;
    greenSignal = new CountDownLatch(threads);
    tp = Executors.newFixedThreadPool(threads);
    this.threads = threads;
    this.iterationsPerThread = iterationsPerThread;
    System.out.println("Starting with threads: " + threads);

    ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
    reporter.start(5, TimeUnit.SECONDS);
  }

  public void start()
      throws HiveException, ExecutionException, InterruptedException {

    // get relevant table and partitions
    fetchTableAndPartitions();
    List<Partition> partitions = partitionsList
        .subList(0, Math.min(numPartitionsToFetch, partitionsList.size()));
    System.out.println("Using partitions: " + partitions.size());

    long sTime = System.currentTimeMillis();
    List<Future<Void>> futureList = new LinkedList<>();
    for (int i = 0; i < threads; i++) {
      futureList.add(tp.submit(
          new Worker(greenSignal, table, partitions, iterationsPerThread)));
    }
    for (Future<Void> f : futureList) {
      f.get();
    }
    long eTime = System.currentTimeMillis();

    System.out.println("Overall time: " + (eTime - sTime) + " ms");
    System.out.println("Avg response time: "
        + (eTime - sTime) * 1.0 / (threads * iterationsPerThread));
    tp.shutdownNow();
  }

  private void fetchTableAndPartitions() throws HiveException {
    HiveConf conf = new HiveConf();
    System.out.println("Connecting to HMS: " + conf.get("hive.metastore.uris"));
    Hive hive = Hive.get(conf);

    //Get the table
    table = hive.getTable(dbName, tableName);
    System.out.println("Table: " + table.getTableName());

    //Get the set of partitions for the table
    partitionsList = hive.getPartitions(table);
    System.out.println("Parts size: " + ((partitionsList == null) ? 0 :
        partitionsList.size()));
  }

  public static void main(String[] args)
      throws InterruptedException, ExecutionException, HiveException {
    if (args.length != 4) {
      System.out.println("args: dbName tblName threads iterationsPerThread "
          + "numPartitionsToFetch");
      System.out.println("E.g ava -cp ./conf/:`hadoop classpath`:/usr/hdp/3.0.0.0-698/hive/lib/*:./: LockBenchmark tpcds_bin_partitioned_acid_orc_30000 store_sales 1 100 1");
    }
    String dbName = args[0];
    String tblName = args[1];
    int threads = Integer.parseInt(args[2]);
    int iterationsPerThread = Integer.parseInt(args[3]);
    int numPartitions = Integer.parseInt(args[4]);
    LockBenchmark benchmark =
        new LockBenchmark(threads, iterationsPerThread, dbName, tblName,
            numPartitions);
    benchmark.start();
  }

  static class Worker implements Callable<Void> {

    private final CountDownLatch greenSignal;
    private final Table table;
    private final List<Partition> partitionList;
    private final int iterations;

    public Worker(CountDownLatch greenSignal, Table tbl,
        List<Partition> partitionList, int iterations) {
      this.greenSignal = greenSignal;
      this.table = tbl;
      this.partitionList = partitionList;
      this.iterations = iterations;

    }

    @Override
    public Void call() throws Exception {
      // create a client
      HiveConf conf = new HiveConf();
      LockClient client = new LockClient(conf);
      client.addTable(table);
      client.addPartitionInput(partitionList);
      long threadId = Thread.currentThread().getId();

      Timer resTimes = metrics.timer("responseTimes_" + threadId);

      // all init done. Time to go
      greenSignal.countDown();
      greenSignal.await();

      for (int i = 0; i < iterations; i++) {
        System.out.println(threadId + ". Start running query. Iteration: " + i);
        Timer.Context timerContext = resTimes.time();
        client.runQuery();
        timerContext.stop();
      }
      return null;
    }
  }
}
