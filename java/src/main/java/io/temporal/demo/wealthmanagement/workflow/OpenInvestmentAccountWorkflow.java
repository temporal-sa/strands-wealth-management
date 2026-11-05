package io.temporal.demo.wealthmanagement.workflow;

import io.temporal.demo.wealthmanagement.data.WealthManagementClient;
import io.temporal.demo.wealthmanagement.model.OpenInvestmentAccountInput;
import io.temporal.demo.wealthmanagement.model.OpenInvestmentAccountOutput;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.util.Map;

/**
 * The same workflow type, query, updates and signals as the Python OpenInvestmentAccountWorkflow
 * in src/temporal_supervisor/workflows/open_account_workflow.py, so the supervisor workflow and the
 * open-account activities drive either version the same way.
 */
@WorkflowInterface
@SuppressWarnings("MethodName") // the method names are the Temporal names the Python side uses
public interface OpenInvestmentAccountWorkflow {
    @WorkflowMethod
    OpenInvestmentAccountOutput run(OpenInvestmentAccountInput inputs);

    @QueryMethod
    String get_current_state();

    @UpdateMethod
    WealthManagementClient get_client_details();

    @UpdateMethod
    String update_client_details(Map<String, Object> clientFields);

    @SignalMethod
    void verify_kyc();

    @SignalMethod
    void compliance_approved();
}
