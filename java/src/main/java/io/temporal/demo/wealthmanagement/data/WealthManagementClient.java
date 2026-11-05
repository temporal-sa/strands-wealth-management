package io.temporal.demo.wealthmanagement.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/** The client record returned by the Python get_client activity (WealthManagementClient in activities/clients.py). */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class WealthManagementClient {
    @JsonProperty("client_id")
    private String clientId;
    @JsonProperty("first_name")
    private String firstName;
    @JsonProperty("last_name")
    private String lastName;
    private String address;
    private String phone;
    private String email;
    @JsonProperty("marital_status")
    private String maritalStatus;
}
