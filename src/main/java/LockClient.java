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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.HashSet;
import java.util.List;

public class LockClient {

  private HiveTxnManager txnMgr;
  private Context ctx;
  private HashSet<ReadEntity> readEntities;

  public LockClient(HiveConf conf) throws Exception {
    conf.set("hive.execution.engine", "MR");
    SessionState.start(conf);
    SessionState ss = SessionState.get();
    ss.initTxnMgr(conf);
    ctx = new Context(conf);

    readEntities = new HashSet<ReadEntity>();
    txnMgr = ss.getTxnMgr();
  }

  public void addTable(Table tbl) {
    readEntities.add(new ReadEntity(tbl));
  }

  public void addPartitionInput(List<Partition> partitions) throws Exception {
    for (Partition p : partitions) {
      readEntities.add(new ReadEntity(p));
    }
  }

  public void runQuery()
      throws LockException {
    QueryPlan qb = new DummyQueryPlan(readEntities, HiveOperation.QUERY);

    txnMgr.openTxn(ctx, "hive");
    txnMgr.acquireLocks(qb, ctx, "hive");
    System.out.println("Got locks");

    List<HiveLock> locks = txnMgr.getLockManager().getLocks(false, false);
    if (locks != null) {
      System.out.println("Lock size: " + locks.size());
      for (HiveLock lock : locks) {
        System.out.println(lock.toString());
      }
    }
    //txnMgr.commitTxn();
    //TODO: This would rollback/close/release locks.
    txnMgr.closeTxnManager();
  }

  private static class DummyQueryPlan extends QueryPlan {
    private final HashSet<ReadEntity> inputs = new HashSet<>();
    private final HashSet<WriteEntity> outputs = new HashSet<>();
    private final String queryId;

    DummyQueryPlan(HashSet<ReadEntity> readEntities, HiveOperation operation) {
      super(operation);
      inputs.addAll(readEntities);
      queryId = QueryPlan.makeQueryId();
    }

    @Override
    public HashSet<ReadEntity> getInputs() {
      return inputs;
    }

    @Override
    public HashSet<WriteEntity> getOutputs() {
      return outputs;
    }

    @Override
    public String getQueryId() {
      return queryId;
    }
  }
}
