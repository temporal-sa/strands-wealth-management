package io.temporal.demo.wealthmanagement;

import io.temporal.client.WorkflowClient;
import io.temporal.demo.wealthmanagement.common.ServerInfo;
import io.temporal.demo.wealthmanagement.workflow.OpenInvestmentAccountWorkflowImpl;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Runs the Java OpenInvestmentAccountWorkflow on the Open Account task queue
 * (TEMPORAL_TASK_QUEUE_OPEN_ACCOUNT). Its activities are the Python ones, on the Python
 * worker's task queue (TEMPORAL_TASK_QUEUE), so this worker registers no activities.
 *
 * The WorkflowClient comes from temporal-spring-boot-starter (application.yaml, or
 * application-tc.yaml for Temporal Cloud), with the claim check codec added when
 * USE_CLAIM_CHECK=true (claimcheck/TemporalOptionsConfig).
 */
@SpringBootApplication
public class StartTheWorker implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(StartTheWorker.class);

    @Autowired
    private WorkflowClient client;

    public static void main(String[] args) {
        SpringApplication.run(StartTheWorker.class, args);
    }

    @Override
    public void run(String... args) {
        log.info("Starting the Open Account worker: {}", ServerInfo.getServerInfo());
        WorkerFactory workerFactory = WorkerFactory.newInstance(client);
        Worker worker = workerFactory.newWorker(ServerInfo.getTaskqueueOpenAccount());
        worker.registerWorkflowImplementationTypes(OpenInvestmentAccountWorkflowImpl.class);
        workerFactory.start();
        log.info("Worker polling task queue '{}'; activities run on '{}'. Ctrl+C to exit.",
                ServerInfo.getTaskqueueOpenAccount(), ServerInfo.getTaskqueue());
    }
}
