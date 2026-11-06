package io.temporal.demo.wealthmanagement.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.demo.wealthmanagement.common.RetryPolicies;
import io.temporal.demo.wealthmanagement.common.ServerInfo;
import io.temporal.demo.wealthmanagement.data.UpdateAccountOpeningStateInput;
import io.temporal.demo.wealthmanagement.data.WealthManagementClient;
import io.temporal.demo.wealthmanagement.model.InvestmentAccount;
import io.temporal.demo.wealthmanagement.model.OpenInvestmentAccountInput;
import io.temporal.demo.wealthmanagement.model.OpenInvestmentAccountOutput;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.ActivityStub;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInfo;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Java version of OpenInvestmentAccountWorkflow (src/temporal_supervisor/workflows/open_account_workflow.py).
 *
 * Opens an investment account, gating on KYC and a human compliance approval, then runs a saga that
 * creates, funds and registers the account, undoing the completed steps if a later one fails. Every
 * state change is signalled to the parent chat workflow. The states, messages, transfer IDs and
 * activity calls are the same as in the Python version.
 *
 * The workflow has no business logic of its own: it calls the Python activities by name on the Python
 * worker's task queue, so the chaos panel's flags apply unchanged.
 */
@SuppressWarnings("MethodName") // the method names are the Temporal names the Python side uses
public class OpenInvestmentAccountWorkflowImpl implements OpenInvestmentAccountWorkflow {
    // Welcome-bonus promotion: a percentage of the initial deposit, up to MAX_BONUS.
    // *** Bug-In-Workflow-Code ***
    // Intentional bug for the "Bug in workflow code" demo: the promo code is "WELCOME-BONUS" (hyphen),
    // but BONUS_RATES is keyed by "WELCOME_BONUS" (underscore), so the lookup in welcomeBonus returns null.
    // To fix it, uncomment the first line below and comment out the second.
    // private static final Map<String, Double> BONUS_RATES = Map.of("WELCOME-BONUS", 0.01);
    private static final Map<String, Double> BONUS_RATES = Map.of("WELCOME_BONUS", 0.01);
    private static final double MAX_BONUS = 500.0;

    private final Logger log = Workflow.getLogger(OpenInvestmentAccountWorkflowImpl.class);

    private WealthManagementClient client;
    private OpenInvestmentAccountInput inputs;
    private boolean initialized = false;
    private boolean kycVerified = false;
    private boolean complianceReviewed = false;
    private String currentState = "Initializing";

    // The activities are implemented in Python, so they are called by name on the Python task queue.
    // The start-to-close timeout bounds a single attempt only, so an outage is retried for as long as it lasts.
    private final ActivityStub activities = Workflow.newUntypedActivityStub(
            ActivityOptions.newBuilder()
                    .setTaskQueue(ServerInfo.getTaskqueue())
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryPolicies.TOOL_RETRY_OPTIONS)
                    .build());

    /** A completed saga step and the activity call that undoes it. */
    private record Compensation(String description, String activityName, List<Object> args) {
    }

    @Override
    public OpenInvestmentAccountOutput run(OpenInvestmentAccountInput inputs) {
        log.info("OpenInvestmentAccountWorkflow started: {}", inputs);
        this.inputs = inputs;

        this.client = activities.execute("get_client", WealthManagementClient.class, inputs.getClient_id());
        this.initialized = true;

        setState("Waiting KYC");
        Workflow.await(() -> this.kycVerified);

        setState("Waiting Compliance Review");
        Workflow.await(() -> this.complianceReviewed);

        setState("Compliance review has been approved. Creating Investment Account");
        return openAccount();
    }

    /**
     * Saga: create, fund, and register the account.
     *
     * Each completed step records how to undo it. If a later step fails with a non-retryable
     * error, the recorded compensations run in reverse order. Transient failures never get here:
     * activities retry until they succeed.
     */
    @SuppressWarnings("unchecked")
    private OpenInvestmentAccountOutput openAccount() {
        String clientId = inputs.getClient_id();
        double amount = inputs.getInitial_amount();
        double bonus = inputs.getPromo_code() != null ? welcomeBonus(inputs) : 0.0;
        // Idempotency key: stable across activity retries and worker restarts. It includes the
        // run ID because the workflow ID is reused when the customer opens an account with the
        // same name again (e.g. after a rollback).
        String transferId = Workflow.getInfo().getWorkflowId() + "-" + Workflow.getInfo().getRunId() + "-initial-deposit";
        List<Compensation> compensations = new ArrayList<>();
        try {
            setState("Creating the account record.");
            Map<String, Object> account = activities.execute("open_investment", HashMap.class,
                    new InvestmentAccount(clientId, inputs.getAccount_name(), amount + bonus));
            String investmentId = (String) account.get("investment_id");
            compensations.add(new Compensation("removing the account record",
                    "remove_investment", List.of(clientId, investmentId)));

            setState(String.format(Locale.US, "Transferring $%,.2f from the linked bank account.", amount));
            activities.execute("debit_linked_account", HashMap.class, clientId, amount, transferId);
            compensations.add(new Compensation(
                    String.format(Locale.US, "refunding $%,.2f to the linked bank account", amount),
                    "credit_linked_account", List.of(clientId, amount, transferId + "-refund")));

            setState("Registering the account with the custodian.");
            activities.execute("register_account", String.class, clientId, investmentId, amount);
        } catch (ActivityFailure e) {
            String reason = reason(e);
            setState("Failed: " + reason + ". Rolling back.");
            for (int i = compensations.size() - 1; i >= 0; i--) {
                Compensation compensation = compensations.get(i);
                setState("Rolling back: " + compensation.description() + ".");
                // Compensations are activities too, so they are retried until they succeed
                activities.execute(compensation.activityName(), Object.class, compensation.args().toArray());
            }
            setState("Rolled back. The account was not opened: " + reason + ".");
            return output(false, "The account was not opened: " + reason);
        }

        setState("Complete");
        return output(true, "investment account created");
    }

    private static double welcomeBonus(OpenInvestmentAccountInput inputs) {
        double rate = BONUS_RATES.get(inputs.getPromo_code());
        return Math.min(inputs.getInitial_amount() * rate, MAX_BONUS);
    }

    // The business reason for a non-retryable activity failure, such as the custodian rejecting the account
    private static String reason(ActivityFailure e) {
        Throwable cause = e.getCause();
        if (cause instanceof ApplicationFailure applicationFailure) {
            return applicationFailure.getOriginalMessage();
        }
        return cause != null ? cause.getMessage() : e.getMessage();
    }

    private static OpenInvestmentAccountOutput output(boolean accountCreated, String message) {
        OpenInvestmentAccountOutput result = new OpenInvestmentAccountOutput();
        result.setAccount_created(accountCreated);
        result.setMessage(message);
        return result;
    }

    @Override
    public String get_current_state() {
        return this.currentState;
    }

    @Override
    public WealthManagementClient get_client_details() {
        Workflow.await(() -> this.initialized);
        return this.client;
    }

    @Override
    public String update_client_details(Map<String, Object> clientFields) {
        // Saves the changes through the Python update_client activity; changing the details also completes KYC
        String result = activities.execute("update_client", String.class, this.inputs.getClient_id(), clientFields);
        this.kycVerified = true;
        return result;
    }

    @Override
    public void verify_kyc() {
        Workflow.await(() -> this.initialized);
        this.kycVerified = true;
    }

    @Override
    public void compliance_approved() {
        Workflow.await(() -> this.initialized && this.kycVerified);
        this.complianceReviewed = true;
    }

    private void setState(String state) {
        this.currentState = state;
        WorkflowInfo info = Workflow.getInfo();
        Optional<String> parentWorkflowId = info.getParentWorkflowId();
        if (parentWorkflowId.isEmpty()) {
            return;
        }
        // Signal the parent chat workflow directly, like the Python version (the task queue doesn't matter)
        Workflow.newUntypedExternalWorkflowStub(parentWorkflowId.get()).signal("update_account_opening_state",
                new UpdateAccountOpeningStateInput(this.inputs.getAccount_name(), state, info.getWorkflowId()));
    }
}
