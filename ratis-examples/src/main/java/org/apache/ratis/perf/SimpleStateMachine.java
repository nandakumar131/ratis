/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ratis.perf;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class SimpleStateMachine extends BaseStateMachine {

  @Override
  public Message applyTransactionSync(TransactionContext trx)
      throws ExecutionException, InterruptedException {
    long startTime = System.currentTimeMillis();
    // return the same message contained in the entry
    final RaftProtos.LogEntryProto entry = Objects.requireNonNull(trx.getLogEntry());
    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
    long endTime = System.currentTimeMillis();
    stateMachineTime.addAndGet(endTime - startTime);
    return  Message.valueOf(entry.getStateMachineLogEntry().getLogData());
  }
}
