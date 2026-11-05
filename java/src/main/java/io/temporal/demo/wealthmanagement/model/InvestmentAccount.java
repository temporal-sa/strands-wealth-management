package io.temporal.demo.wealthmanagement.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/** Argument of the Python open_investment activity (InvestmentAccount in src/common/investment_manager.py). */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class InvestmentAccount {
    @JsonProperty("client_id")
    private String clientId;
    private String name;
    private double balance;
}
