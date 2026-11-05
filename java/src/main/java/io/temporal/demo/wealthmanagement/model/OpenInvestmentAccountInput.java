package io.temporal.demo.wealthmanagement.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** The workflow input (OpenInvestmentAccountInput in workflows/open_account_workflow.py). */
@Getter
@Setter
@ToString
@SuppressWarnings("MemberName") // the field names are the JSON names the Python side uses
public class OpenInvestmentAccountInput {
    private String client_id;
    private String account_name;
    private double initial_amount;
    // Set when a promotion is running for the new account (e.g. "WELCOME-BONUS")
    private String promo_code;
}
