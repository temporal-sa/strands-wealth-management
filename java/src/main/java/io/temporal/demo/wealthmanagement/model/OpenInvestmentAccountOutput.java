package io.temporal.demo.wealthmanagement.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** The workflow result (OpenInvestmentAccountOutput in workflows/open_account_workflow.py). */
@Getter
@Setter
@ToString
@SuppressWarnings("MemberName") // the field names are the JSON names the Python side uses
public class OpenInvestmentAccountOutput {
    private boolean account_created;
    private String message;
}
