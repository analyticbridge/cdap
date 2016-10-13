/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.test.app;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpContentProducer;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.test.RevealingTxSystemClient;
import co.cask.cdap.test.RevealingTxSystemClient.RevealingTransaction;
import com.google.common.base.Throwables;
import org.apache.http.entity.ContentType;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * An app that starts transactions with custom timeout and validates the timeout using a custom dataset.
 * This app also has methods with @TransactionPolicy annotations, to validate that these don't get run inside a tx.
 * These methods will then start transactions explicitly, and attempt to nest transactions.
 *
 * This relies on TestBase to inject {@link RevealingTxSystemClient} for this test.
 */
@SuppressWarnings("WeakerAccess")
public class AppWithCustomTx extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(AppWithCustomTx.class);

  private static final String NAME = "AppWithCustomTx";
  static final String CAPTURE = "capture";
  static final String DEFAULT = "default";

  static final String ACTION_TX = "TxAction";
  static final String ACTION_NOTX = "NoTxAction";
  static final String CONSUMER = "HttpContentConsumer";
  static final String PRODUCER = "HttpContentProducer";
  static final String SERVICE = "TimedTxService";
  static final String WORKER = "TimedTxWorker";
  static final String WORKFLOW_TX = "TxWorkflow";
  static final String WORKFLOW_NOTX = "NoTxWorkflow";

  static final String INITIALIZE = "initialize";
  static final String INITIALIZE_TX = "initialize-tx";
  static final String INITIALIZE_NEST = "initialize-nest";
  static final String DESTROY = "destroy";
  static final String DESTROY_TX = "destroy-tx";
  static final String DESTROY_NEST = "destroy-nest";
  static final String RUNTIME = "runtime";
  static final String RUNTIME_TX = "runtime-tx";
  static final String RUNTIME_NEST = "runtime-nest";

  static final int TIMEOUT_ACTION_RUNTIME = 13;
  static final int TIMEOUT_ACTION_DESTROY = 14;
  static final int TIMEOUT_ACTION_INITIALIZE = 15;
  static final int TIMEOUT_CONSUMER_RUNTIME = 16;
  static final int TIMEOUT_PRODUCER_RUNTIME = 21;
  static final int TIMEOUT_WORKER_DESTROY = 24;
  static final int TIMEOUT_WORKER_INITIALIZE = 25;
  static final int TIMEOUT_WORKER_RUNTIME = 26;
  static final int TIMEOUT_WORKFLOW_DESTROY = 27;
  static final int TIMEOUT_WORKFLOW_INITIALIZE = 28;

  @Override
  public void configure() {
    setName(NAME);
    createDataset(CAPTURE, TransactionCapturingTable.class);
    addWorker(new TimeoutWorker());
    addWorkflow(new TxWorkflow());
    addWorkflow(new NoTxWorkflow());
    addService(new AbstractService() {
      @Override
      protected void configure() {
        setName(SERVICE);
        addHandler(new TimeoutHandler());
      }
    });
  }

  /**
   * Uses the provided Transactional with the given timeout, and records the timeout that the transaction
   * was actually given, or "default" if no explicit timeout was given.
   */
  static void executeRecordTransaction(Transactional transactional,
                                       final String row, final String column, int timeout) {
    try {
      transactional.execute(timeout, new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          recordTransaction(context, row, column);
        }
      });
    } catch (TransactionFailureException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * If in a transaction, records the timeout that the current transaction was given, or "default" if no explicit
   * timeout was given. Otherwise does nothing.
   *
   * Note: we know whether and what explicit timeout was given, because we inject a {@link RevealingTxSystemClient},
   *       which returns a {@link RevealingTransaction} for {@link TransactionSystemClient#startShort(int)} only.
   */
  static void recordTransaction(DatasetContext context, String row, String column) {
    TransactionCapturingTable capture = context.getDataset(CAPTURE);
    Transaction tx = capture.getTx();
    if (tx == null) {
      return;
    }
    // we cannot cast because the RevealingTransaction is not visible in the program class loader
    String value = DEFAULT;
    if ("RevealingTransaction".equals(tx.getClass().getSimpleName())) {
      int txTimeout;
      try {
        txTimeout = (int) tx.getClass().getField("timeout").get(tx);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
      value = String.valueOf(txTimeout);
    }
    capture.getTable().put(new Put(row, column, value));
  }

  /**
   * Attempt to nest transactions. we expect this to fail, but we catch the exception and leave it to the
   * main test method to validate that no transaction was recorded.
   */
  static void attemptNestedTransaction(Transactional txnl, final String row, final String key) {
    try {
      txnl.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          recordTransaction(ctext, row, key);
        }
      });
      LOG.error("Nested transaction should not have succeeded for {}:{}", row, key);
    } catch (TransactionFailureException e) {
      // expected: starting nested transaction should fail
      LOG.info("Nested transaction failed as expected for {}:{}", row, key);
    } catch (RuntimeException e) {
      // TODO (CDAP-6837): this is needed because worker's execute() propagates the tx failure as a runtime exception
      if (e.getCause() instanceof TransactionFailureException) {
        // expected: starting nested transaction should fail
        LOG.info("Nested transaction failed as expected for {}:{}", row, key);
      } else {
        throw e;
      }
    }
  }

  public static class TimeoutWorker extends AbstractWorker {

    @Override
    protected void configure() {
      setName(WORKER);
    }

    @Override
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);
      recordTransaction(getContext(), WORKER, INITIALIZE);
      executeRecordTransaction(context, WORKER, INITIALIZE_TX, TIMEOUT_WORKER_INITIALIZE);
      context.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), WORKER, INITIALIZE_NEST);
        }
      });
    }

    @Override
    public void run() {
      recordTransaction(getContext(), WORKER, RUNTIME);
      executeRecordTransaction(getContext(), WORKER, RUNTIME_TX, TIMEOUT_WORKER_RUNTIME);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), WORKER, RUNTIME_NEST);
        }
      });
    }

    @Override
    public void destroy() {
      recordTransaction(getContext(), WORKER, DESTROY);
      executeRecordTransaction(getContext(), WORKER, DESTROY_TX, TIMEOUT_WORKER_DESTROY);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), WORKER, DESTROY_NEST);
        }
      });
      super.destroy();
    }
  }

  public static class TimeoutHandler extends AbstractHttpServiceHandler {

    // service context does not have Transactional, no need to test lifecycle methods

    @PUT
    @Path("test")
    public HttpContentConsumer handle(HttpServiceRequest request, HttpServiceResponder responder) {
      return new HttpContentConsumer() {

        @Override
        public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
          executeRecordTransaction(transactional, CONSUMER, RUNTIME_TX, TIMEOUT_CONSUMER_RUNTIME);
        }

        @Override
        public void onFinish(HttpServiceResponder responder) throws Exception {
          responder.send(200, new HttpContentProducer() {

            @Override
            public ByteBuffer nextChunk(Transactional transactional) throws Exception {
              executeRecordTransaction(transactional, PRODUCER, RUNTIME_TX, TIMEOUT_PRODUCER_RUNTIME);
              return ByteBuffer.allocate(0);
            }

            @Override
            public void onFinish() throws Exception {
            }

            @Override
            public void onError(Throwable failureCause) {
            }
          }, ContentType.TEXT_PLAIN.getMimeType());
        }

        @Override
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
        }
      };
    }
  }

  private static class TxWorkflow extends AbstractWorkflow {
    @Override
    protected void configure() {
      setName(WORKFLOW_TX);
      addAction(new TxAction());
    }

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      recordTransaction(context, WORKFLOW_TX, INITIALIZE);
      attemptNestedTransaction(context, WORKFLOW_TX, INITIALIZE_NEST);
    }

    @Override
    public void destroy() {
      super.destroy();
      recordTransaction(getContext(), WORKFLOW_TX, DESTROY);
      attemptNestedTransaction(getContext(), WORKFLOW_TX, DESTROY_NEST);
    }
  }

  private static class NoTxWorkflow extends AbstractWorkflow {
    @Override
    protected void configure() {
      setName(WORKFLOW_NOTX);
      addAction(new NoTxAction());
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      recordTransaction(context, WORKFLOW_NOTX, INITIALIZE);
      executeRecordTransaction(getContext(), WORKFLOW_NOTX, INITIALIZE_TX, TIMEOUT_WORKFLOW_INITIALIZE);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), WORKFLOW_NOTX, INITIALIZE_NEST);
        }
      });
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void destroy() {
      super.destroy();
      recordTransaction(getContext(), WORKFLOW_NOTX, DESTROY);
      executeRecordTransaction(getContext(), WORKFLOW_NOTX, DESTROY_TX, TIMEOUT_WORKFLOW_DESTROY);
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext ctext) throws Exception {
            attemptNestedTransaction(getContext(), WORKFLOW_NOTX, DESTROY_NEST);
          }
        });
      } catch (TransactionFailureException e) {
        throw Throwables.propagate(e.getCause() == null ? e : e.getCause());
      }
    }
  }

  public static class NoTxAction extends AbstractCustomAction {

    @Override
    protected void configure() {
      setName(ACTION_NOTX);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    protected void initialize() throws Exception {
      recordTransaction(getContext(), ACTION_NOTX, INITIALIZE);
      executeRecordTransaction(getContext(), ACTION_NOTX, INITIALIZE_TX, TIMEOUT_ACTION_INITIALIZE);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), ACTION_NOTX, INITIALIZE_NEST);
        }
      });
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void destroy() {
      recordTransaction(getContext(), ACTION_NOTX, DESTROY);
      executeRecordTransaction(getContext(), ACTION_NOTX, DESTROY_TX, TIMEOUT_ACTION_DESTROY);
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext ctext) throws Exception {
            attemptNestedTransaction(getContext(), ACTION_NOTX, DESTROY_NEST);
          }
        });
      } catch (TransactionFailureException e) {
        throw Throwables.propagate(e.getCause() == null ? e : e.getCause());
      }
    }

    @Override
    public void run() throws Exception {
      recordTransaction(getContext(), ACTION_NOTX, RUNTIME_TX);
      executeRecordTransaction(getContext(), ACTION_NOTX, RUNTIME_TX, TIMEOUT_ACTION_RUNTIME);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), ACTION_NOTX, RUNTIME_NEST);
        }
      });
    }
  }

  public static class TxAction extends AbstractCustomAction {

    @Override
    protected void configure() {
      setName(ACTION_TX);
    }

    @Override
    protected void initialize() throws Exception {
      recordTransaction(getContext(), ACTION_TX, INITIALIZE);
      attemptNestedTransaction(getContext(), ACTION_TX, INITIALIZE_NEST);
    }

    @Override
    public void destroy() {
      recordTransaction(getContext(), ACTION_TX, DESTROY);
      attemptNestedTransaction(getContext(), ACTION_TX, DESTROY_NEST);
    }

    @Override
    public void run() throws Exception {
      recordTransaction(getContext(), ACTION_TX, RUNTIME_TX);
      executeRecordTransaction(getContext(), ACTION_TX, RUNTIME_TX, TIMEOUT_ACTION_RUNTIME);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), ACTION_TX, RUNTIME_NEST);
        }
      });
    }
  }
}
