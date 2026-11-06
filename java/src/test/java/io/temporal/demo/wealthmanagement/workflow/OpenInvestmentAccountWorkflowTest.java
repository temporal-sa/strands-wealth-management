package io.temporal.demo.wealthmanagement.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityInfo;
import io.temporal.activity.DynamicActivity;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.converter.EncodedValues;
import io.temporal.demo.wealthmanagement.common.ServerInfo;
import io.temporal.demo.wealthmanagement.data.UpdateAccountOpeningStateInput;
import io.temporal.demo.wealthmanagement.data.WealthManagementClient;
import io.temporal.demo.wealthmanagement.model.InvestmentAccount;
import io.temporal.demo.wealthmanagement.model.OpenInvestmentAccountInput;
import io.temporal.demo.wealthmanagement.model.OpenInvestmentAccountOutput;
import io.temporal.failure.ApplicationFailure;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import io.temporal.workflow.Async;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Promise;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the Java OpenInvestmentAccountWorkflow against stubs of the Python activities it calls by
 * name, mirroring tests/test_failure_scenarios.py. As with the split task queues in the demo, the
 * workflow runs on its own task queue and the stub activities on the Python worker's task queue.
 */
class OpenInvestmentAccountWorkflowTest {
    private static final String OPEN_ACCOUNT_QUEUE = "open-account-java-test";
    private static final String CLIENT_ID = "C-1";
    private static final String INVESTMENT_ID = "i-test";

    // Activity calls, in order
    private static final List<String> CALLS = Collections.synchronizedList(new ArrayList<>());
    // Activity name -> "reject" (non-retryable failure) or a number of transient failures
    private static final Map<String, Object> FAILURES = new HashMap<>();
    // Transfer IDs passed to debit_linked_account / credit_linked_account, in order
    private static final List<String> TRANSFER_IDS = Collections.synchronizedList(new ArrayList<>());
    // Arguments of the last open_investment / update_client calls
    private static final Map<String, Object> LAST_ARGS = Collections.synchronizedMap(new HashMap<>());

    private TestWorkflowEnvironment env;
    private WorkflowClient client;

    /** Stands in for the Python activities, which the workflow calls through an untyped stub. */
    public static class StubPythonActivities implements DynamicActivity {
        @Override
        @SuppressWarnings("unchecked")
        public Object execute(EncodedValues args) {
            ActivityInfo info = Activity.getExecutionContext().getInfo();
            String name = info.getActivityType();
            int attempt = info.getAttempt();
            switch (name) {
                case "get_client" -> {
                    CALLS.add(name);
                    return new WealthManagementClient(args.get(0, String.class), "Sam", "Doe", "1 Main St",
                            "555", "s@example.com", "single");
                }
                case "update_client" -> {
                    CALLS.add("update_client:" + args.get(0, String.class));
                    LAST_ARGS.put("update_client", args.get(1, Map.class));
                    return "Client information successfully updated";
                }
                case "open_investment" -> {
                    CALLS.add(name + "#" + attempt);
                    LAST_ARGS.put("open_investment", args.get(0, InvestmentAccount.class));
                    maybeFail(name, attempt);
                    return Map.of("investment_id", INVESTMENT_ID, "name", "Test", "balance", 500.0);
                }
                case "debit_linked_account" -> {
                    CALLS.add("debit:" + args.get(1, Double.class));
                    TRANSFER_IDS.add(args.get(2, String.class));
                    maybeFail(name, attempt);
                    return Map.of();
                }
                case "credit_linked_account" -> {
                    CALLS.add("credit:" + args.get(1, Double.class));
                    TRANSFER_IDS.add(args.get(2, String.class));
                    return Map.of();
                }
                case "register_account" -> {
                    CALLS.add("register");
                    maybeFail(name, attempt);
                    return "CUST-1";
                }
                case "remove_investment" -> {
                    CALLS.add("remove_investment:" + args.get(1, String.class));
                    return true;
                }
                default -> throw ApplicationFailure.newNonRetryableFailure("Unexpected activity " + name, "Unexpected");
            }
        }

        private static void maybeFail(String name, int attempt) {
            Object behavior = FAILURES.get(name);
            if ("reject".equals(behavior)) {
                throw ApplicationFailure.newNonRetryableFailure(name + " rejected", "Rejected");
            }
            if (behavior instanceof Integer transientFailures && attempt <= transientFailures) {
                throw ApplicationFailure.newFailure(name + " unavailable", "ConnectionError");
            }
        }
    }

    /** Stands in for the Python supervisor workflow: starts the child and records its status signals. */
    @WorkflowInterface
    public interface TestParentWorkflow {
        @WorkflowMethod
        List<UpdateAccountOpeningStateInput> run(OpenInvestmentAccountInput input, String childWorkflowId);

        @SignalMethod(name = "update_account_opening_state")
        void updateAccountOpeningState(UpdateAccountOpeningStateInput input);
    }

    public static class TestParentWorkflowImpl implements TestParentWorkflow {
        private final List<UpdateAccountOpeningStateInput> states = new ArrayList<>();

        @Override
        public List<UpdateAccountOpeningStateInput> run(OpenInvestmentAccountInput input, String childWorkflowId) {
            OpenInvestmentAccountWorkflow child = Workflow.newChildWorkflowStub(OpenInvestmentAccountWorkflow.class,
                    ChildWorkflowOptions.newBuilder()
                            .setWorkflowId(childWorkflowId)
                            .setTaskQueue(OPEN_ACCOUNT_QUEUE)
                            .build());
            Promise<OpenInvestmentAccountOutput> result = Async.function(child::run, input);
            result.get();
            // The last signal can arrive after the child completes
            Workflow.await(Duration.ofSeconds(5), () -> !states.isEmpty() && "Complete".equals(states.get(states.size() - 1).getState()));
            return states;
        }

        @Override
        public void updateAccountOpeningState(UpdateAccountOpeningStateInput input) {
            states.add(input);
        }
    }

    @BeforeEach
    void setUp() {
        CALLS.clear();
        FAILURES.clear();
        TRANSFER_IDS.clear();
        LAST_ARGS.clear();
        env = TestWorkflowEnvironment.newInstance();
        Worker workflowWorker = env.newWorker(OPEN_ACCOUNT_QUEUE);
        workflowWorker.registerWorkflowImplementationTypes(OpenInvestmentAccountWorkflowImpl.class,
                TestParentWorkflowImpl.class);
        Worker pythonWorker = env.newWorker(ServerInfo.getTaskqueue());
        pythonWorker.registerActivitiesImplementations(new StubPythonActivities());
        env.start();
        client = env.getWorkflowClient();
    }

    @AfterEach
    void tearDown() {
        env.close();
    }

    private static OpenInvestmentAccountInput input(String promoCode) {
        OpenInvestmentAccountInput input = new OpenInvestmentAccountInput();
        input.setClient_id(CLIENT_ID);
        input.setAccount_name("Test");
        input.setInitial_amount(500.0);
        input.setPromo_code(promoCode);
        return input;
    }

    private OpenInvestmentAccountWorkflow start(String workflowId, String promoCode) {
        OpenInvestmentAccountWorkflow workflow = client.newWorkflowStub(OpenInvestmentAccountWorkflow.class,
                WorkflowOptions.newBuilder().setWorkflowId(workflowId).setTaskQueue(OPEN_ACCOUNT_QUEUE).build());
        WorkflowClient.start(workflow::run, input(promoCode));
        return workflow;
    }

    private OpenInvestmentAccountWorkflow startAndApprove(String workflowId, String promoCode) {
        OpenInvestmentAccountWorkflow workflow = start(workflowId, promoCode);
        workflow.verify_kyc();
        workflow.compliance_approved();
        return workflow;
    }

    private OpenInvestmentAccountOutput openAccount(String workflowId) {
        OpenInvestmentAccountWorkflow workflow = startAndApprove(workflowId, null);
        return WorkflowStub.fromTyped(workflow).getResult(OpenInvestmentAccountOutput.class);
    }

    private OpenInvestmentAccountOutput openAccount() {
        return openAccount("open-account-test-" + UUID.randomUUID());
    }

    private String state(String workflowId) {
        return client.newWorkflowStub(OpenInvestmentAccountWorkflow.class, workflowId).get_current_state();
    }

    /** Run ID of the latest run with this workflow ID. */
    private String runId(String workflowId) {
        return client.newUntypedWorkflowStub(workflowId).describe().getExecution().getRunId();
    }

    private List<HistoryEvent> history(String workflowId) {
        return client.fetchHistory(workflowId).getEvents();
    }

    @Test
    void sagaHappyPath() {
        OpenInvestmentAccountOutput result = openAccount("happy-path");
        assertTrue(result.isAccount_created());
        assertEquals("investment account created", result.getMessage());
        assertEquals("Complete", state("happy-path"));
        assertEquals(List.of("get_client", "open_investment#1", "debit:500.0", "register"), CALLS);
        // Same transfer ID as the Python workflow: one per account opening (workflow ID and run ID)
        assertEquals(List.of("happy-path-" + runId("happy-path") + "-initial-deposit"), TRANSFER_IDS);
        InvestmentAccount account = (InvestmentAccount) LAST_ARGS.get("open_investment");
        assertEquals(CLIENT_ID, account.getClientId());
        assertEquals("Test", account.getName());
        assertEquals(500.0, account.getBalance());
    }

    @Test
    void sagaRollsBackInReverseWhenCustodianRejects() {
        FAILURES.put("register_account", "reject");
        OpenInvestmentAccountOutput result = openAccount("custodian-rejects");
        assertFalse(result.isAccount_created());
        assertEquals("The account was not opened: register_account rejected", result.getMessage());
        assertEquals("Rolled back. The account was not opened: register_account rejected.", state("custodian-rejects"));
        assertEquals(List.of("get_client", "open_investment#1", "debit:500.0", "register",
                "credit:500.0", "remove_investment:" + INVESTMENT_ID), CALLS);
        String transferId = "custodian-rejects-" + runId("custodian-rejects") + "-initial-deposit";
        assertEquals(List.of(transferId, transferId + "-refund"), TRANSFER_IDS);
    }

    @Test
    void sagaUndoesOnlyCompletedSteps() {
        FAILURES.put("debit_linked_account", "reject");
        OpenInvestmentAccountOutput result = openAccount();
        assertFalse(result.isAccount_created());
        // The debit never happened, so there is nothing to refund.
        assertEquals(List.of("get_client", "open_investment#1", "debit:500.0",
                "remove_investment:" + INVESTMENT_ID), CALLS);
    }

    @Test
    void transientFailuresAreRetriedNotRolledBack() {
        FAILURES.put("open_investment", 2);
        OpenInvestmentAccountOutput result = openAccount();
        assertTrue(result.isAccount_created());
        assertEquals(List.of("get_client", "open_investment#1", "open_investment#2", "open_investment#3",
                "debit:500.0", "register"), CALLS);
    }

    @Test
    void restartingAWorkflowIdUsesANewTransferId() {
        // The supervisor gives every account opening its own workflow ID
        // (OpenAccount-<chat>-<client>-<account name>-<n>). The transfer ID also includes the run
        // ID, so even if a workflow ID were started again after a rollback, it would be a new
        // transfer, not a repeat of the refunded one.
        String workflowId = "OpenAccount-chat-1-" + CLIENT_ID + "-Vacation-1";
        FAILURES.put("register_account", "reject");
        assertFalse(openAccount(workflowId).isAccount_created());
        String firstRun = runId(workflowId);
        FAILURES.clear();
        assertTrue(openAccount(workflowId).isAccount_created());
        String secondRun = runId(workflowId);
        assertNotEquals(firstRun, secondRun);
        assertEquals(List.of(workflowId + "-" + firstRun + "-initial-deposit",
                workflowId + "-" + firstRun + "-initial-deposit-refund",
                workflowId + "-" + secondRun + "-initial-deposit"), TRANSFER_IDS);
        assertNotEquals(TRANSFER_IDS.get(0), TRANSFER_IDS.get(2));
    }

    @Test
    void updateClientDetailsSavesTheChangesAndCompletesKyc() {
        OpenInvestmentAccountWorkflow workflow = start("update-details", null);

        // get_client_details waits until the workflow has loaded the client
        WealthManagementClient details = workflow.get_client_details();
        assertEquals(CLIENT_ID, details.getClientId());
        assertEquals("Doe", details.getLastName());

        String result = workflow.update_client_details(Map.of("last_name", "Doenut"));
        assertEquals("Client information successfully updated", result);
        assertEquals(Map.of("last_name", "Doenut"), LAST_ARGS.get("update_client"));
        // No verify_kyc signal: the update completed KYC
        assertEquals("Waiting Compliance Review", state("update-details"));

        workflow.compliance_approved();
        assertTrue(WorkflowStub.fromTyped(workflow).getResult(OpenInvestmentAccountOutput.class).isAccount_created());
        assertEquals(List.of("get_client", "update_client:" + CLIENT_ID, "open_investment#1", "debit:500.0", "register"), CALLS);
    }

    @Test
    void everyStateIsSignalledToTheParent() throws InterruptedException {
        String childId = "OpenAccount-parent-test-" + CLIENT_ID + "-Test-1";
        TestParentWorkflow parent = client.newWorkflowStub(TestParentWorkflow.class,
                WorkflowOptions.newBuilder().setWorkflowId("parent-test").setTaskQueue(OPEN_ACCOUNT_QUEUE).build());
        WorkflowClient.start(parent::run, input(null), childId);

        OpenInvestmentAccountWorkflow child = client.newWorkflowStub(OpenInvestmentAccountWorkflow.class, childId);
        for (int i = 0; i < 40 && !"Waiting KYC".equals(currentStateOrNull(child)); i++) {
            Thread.sleep(100);
        }
        child.verify_kyc();
        child.compliance_approved();

        List<UpdateAccountOpeningStateInput> signals = WorkflowStub.fromTyped(parent).getResult(List.class).stream()
                .map(value -> new ObjectMapper().convertValue(value, UpdateAccountOpeningStateInput.class))
                .toList();
        assertEquals(List.of(
                "Waiting KYC",
                "Waiting Compliance Review",
                "Compliance review has been approved. Creating Investment Account",
                "Creating the account record.",
                "Transferring $500.00 from the linked bank account.",
                "Registering the account with the custodian.",
                "Complete"), signals.stream().map(UpdateAccountOpeningStateInput::getState).toList());
        for (UpdateAccountOpeningStateInput signal : signals) {
            assertEquals("Test", signal.getAccountName());
            assertEquals(childId, signal.getWorkflowId());
        }
    }

    private static String currentStateOrNull(OpenInvestmentAccountWorkflow workflow) {
        try {
            return workflow.get_current_state();
        } catch (RuntimeException e) {
            return null; // not started yet
        }
    }

    @Test
    void withoutAParentNothingIsSignalled() {
        assertTrue(openAccount("no-parent").isAccount_created());
        long signals = history("no-parent").stream()
                .filter(event -> event.getEventType() == EventType.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED)
                .count();
        assertEquals(0, signals);
    }

    @Test
    void promoCodePathBugPausesWorkflowInsteadOfFailingIt() throws InterruptedException {
        String workflowId = "promo-bug";
        startAndApprove(workflowId, "WELCOME-BONUS");

        String failure = null;
        for (int i = 0; i < 40 && failure == null; i++) {
            for (HistoryEvent event : history(workflowId)) {
                if (event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED) {
                    // The message is generic; the NullPointerException is in the cause and stack trace
                    failure = event.getWorkflowTaskFailedEventAttributes().getFailure().toString();
                }
            }
            Thread.sleep(250);
        }

        assertNotNull(failure, "the demo bug should fail the workflow task");
        assertTrue(failure.contains("NullPointerException") && failure.contains("welcomeBonus"), failure);
        // Paused, not failed: still running, and the saga never started.
        WorkflowExecutionStatus status = env.getWorkflowServiceStubs().blockingStub()
                .describeWorkflowExecution(DescribeWorkflowExecutionRequest.newBuilder()
                        .setNamespace(env.getNamespace())
                        .setExecution(client.newUntypedWorkflowStub(workflowId).getExecution())
                        .build())
                .getWorkflowExecutionInfo().getStatus();
        assertEquals(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING, status);
        assertFalse(CALLS.contains("open_investment#1"));
        client.newUntypedWorkflowStub(workflowId).terminate("test complete");
    }
}
