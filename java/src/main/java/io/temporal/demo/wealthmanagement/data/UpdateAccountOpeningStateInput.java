package io.temporal.demo.wealthmanagement.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Payload of the parent chat workflow's update_account_opening_state signal
 * (UpdateAccountOpeningStateInput in src/common/account_context.py).
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class UpdateAccountOpeningStateInput {
    @JsonProperty("account_name")
    private String accountName;
    private String state;
    // The open-account workflow that reported the state
    @JsonProperty("workflow_id")
    private String workflowId;
}
