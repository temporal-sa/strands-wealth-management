package io.temporal.demo.wealthmanagement.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.temporal.api.common.v1.Payload;
import io.temporal.common.converter.DataConverter;
import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.common.converter.EncodingKeys;
import io.temporal.demo.wealthmanagement.data.UpdateAccountOpeningStateInput;
import io.temporal.demo.wealthmanagement.data.WealthManagementClient;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The JSON the Java worker reads and writes must be the JSON the Python side (the StrandsPlugin's
 * pydantic data converter) writes and reads. The Python payloads below were produced with
 * temporalio.contrib.pydantic.pydantic_data_converter from the Python dataclasses / models.
 */
class PayloadJsonTest {
    private static final DataConverter CONVERTER = DefaultDataConverter.newDefaultInstance();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Payload pythonPayload(String json) {
        return Payload.newBuilder()
                .putMetadata(EncodingKeys.METADATA_ENCODING_KEY, ByteString.copyFromUtf8("json/plain"))
                .setData(ByteString.copyFromUtf8(json))
                .build();
    }

    private static List<String> fieldNames(Object value) throws Exception {
        Optional<Payload> payload = CONVERTER.toPayload(value);
        assertTrue(payload.isPresent());
        assertEquals("json/plain", payload.get().getMetadataOrThrow(EncodingKeys.METADATA_ENCODING_KEY).toStringUtf8());
        JsonNode json = MAPPER.readTree(payload.get().getData().toStringUtf8());
        List<String> names = new ArrayList<>();
        json.fieldNames().forEachRemaining(names::add);
        names.sort(null);
        return names;
    }

    @Test
    void readsThePythonWorkflowInput() {
        OpenInvestmentAccountInput input = CONVERTER.fromPayload(pythonPayload(
                "{\"client_id\":\"123\",\"account_name\":\"Vacation\",\"initial_amount\":150.78,\"promo_code\":\"WELCOME-BONUS\"}"),
                OpenInvestmentAccountInput.class, OpenInvestmentAccountInput.class);
        assertEquals("123", input.getClient_id());
        assertEquals("Vacation", input.getAccount_name());
        assertEquals(150.78, input.getInitial_amount());
        assertEquals("WELCOME-BONUS", input.getPromo_code());
    }

    @Test
    void readsThePythonClient() {
        WealthManagementClient client = CONVERTER.fromPayload(pythonPayload(
                "{\"client_id\":\"123\",\"first_name\":\"Don\",\"last_name\":\"Doe\",\"address\":\"123 Main\","
                        + "\"phone\":\"555\",\"email\":\"d@x.com\",\"marital_status\":\"married\"}"),
                WealthManagementClient.class, WealthManagementClient.class);
        assertEquals("123", client.getClientId());
        assertEquals("Don", client.getFirstName());
        assertEquals("Doe", client.getLastName());
        assertEquals("married", client.getMaritalStatus());
    }

    @Test
    void readsThePythonResult() {
        OpenInvestmentAccountOutput output = CONVERTER.fromPayload(pythonPayload(
                "{\"account_created\":true,\"message\":\"investment account created\"}"),
                OpenInvestmentAccountOutput.class, OpenInvestmentAccountOutput.class);
        assertTrue(output.isAccount_created());
        assertEquals("investment account created", output.getMessage());
    }

    @Test
    void writesThePythonFieldNames() throws Exception {
        assertEquals(List.of("account_name", "client_id", "initial_amount", "promo_code"),
                fieldNames(new OpenInvestmentAccountInput()));
        assertEquals(List.of("account_created", "message"), fieldNames(new OpenInvestmentAccountOutput()));
        assertEquals(List.of("address", "client_id", "email", "first_name", "last_name", "marital_status", "phone"),
                fieldNames(new WealthManagementClient()));
        assertEquals(List.of("balance", "client_id", "name"), fieldNames(new InvestmentAccount("123", "Vacation", 150.78)));
        assertEquals(List.of("account_name", "state", "workflow_id"),
                fieldNames(new UpdateAccountOpeningStateInput("Vacation", "Complete", "wf-1")));
    }
}
