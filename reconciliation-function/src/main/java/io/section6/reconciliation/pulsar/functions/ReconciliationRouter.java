package io.section6.reconciliation.pulsar.functions;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import io.section6.reconciliation.model.ReconciliationData;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

public class ReconciliationRouter implements Function<ReconciliationData, Void> {
	private String ASTRA_TOKEN = "<replace-me>";
	private static final String ENDPOINT = "https://1e4f081a-c9de-4b95-9607-1ffb6b202b60-australiaeast.apps.astra.datastax.com/api/rest";

//	@Override
//	public void initialize(Context context) {
//		Optional<Object> token = context.getUserConfigValue("astra-token");
//
//		if (token.isEmpty()) {
//			throw new RuntimeException("No astra-token found");
//		}
//
//		this.ASTRA_TOKEN = token.toString();
//	}

	public Void process(ReconciliationData reconciliationData, Context ctx) throws Exception {
		HttpClient client = HttpClient.newHttpClient();
		this.findTransaction(client, reconciliationData);
		return null;
	}

	private void findTransaction(HttpClient client, ReconciliationData reconciliationData) throws Exception {
		String primaryKey = reconciliationData.accountNumber + "/" + reconciliationData.id;

		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(ENDPOINT + "/v2/keyspaces/transactions/transactions_by_account/"+primaryKey))
				.header("Content-Type", "application/json")
				.header("x-cassandra-token", ASTRA_TOKEN)
				.GET()
				.build();

		HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

		JSONObject json = new JSONObject(response.body());

		JSONArray data = json.getJSONArray("data");

		// existing transaction
		if (data.length() == 1) {
			JSONObject transaction = data.getJSONObject(0);

			System.out.println("Compare with existing");

			Set<String> differences = reconciliationData.compareWithTransaction(transaction);

			if (differences.isEmpty()) {
				System.out.println("No differences found");
				return;
			}

			System.out.println("Differences found");
			System.out.println(differences);

			JSONObject toInsert = new JSONObject();

			toInsert.put("accountnumber", reconciliationData.accountNumber);
			toInsert.put("amount", reconciliationData.amount);
			toInsert.put("category", reconciliationData.category);
			toInsert.put("code", reconciliationData.code);
			toInsert.put("description", reconciliationData.description);
			toInsert.put("particular", reconciliationData.particulars);
			toInsert.put("proccesseddate", reconciliationData.processedDate);
			toInsert.put("reference", reconciliationData.reference);
			toInsert.put("status", reconciliationData.status);
			toInsert.put("transactiondate", reconciliationData.transactionDate);
			toInsert.put("transactionid", reconciliationData.id);
			toInsert.put("comments", "Differences found in columns: " + differences);

			this.insertMismatch(client, toInsert);

		} else {
			System.out.println("Missing transaction");
			System.out.println(reconciliationData);

			JSONObject toInsert = new JSONObject();

			toInsert.put("accountnumber", reconciliationData.accountNumber);
			toInsert.put("amount", reconciliationData.amount);
			toInsert.put("category", reconciliationData.category);
			toInsert.put("code", reconciliationData.code);
			toInsert.put("description", reconciliationData.description);
			toInsert.put("particular", reconciliationData.particulars);
			toInsert.put("proccesseddate", reconciliationData.processedDate);
			toInsert.put("reference", reconciliationData.reference);
			toInsert.put("status", reconciliationData.status);
			toInsert.put("transactiondate", reconciliationData.transactionDate);
			toInsert.put("transactionid", reconciliationData.id);
			toInsert.put("comments", "Missing transaction");

			this.insertMismatch(client, toInsert);
		}
	}

	private void insertMismatch(HttpClient client, JSONObject data) throws Exception {
		System.out.println("Insert Mismatch");
		System.out.println(data);

		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(ENDPOINT + "/v2/keyspaces/transactions/reconcilation_transactions_by_account"))
				.header("Content-Type", "application/json")
				.header("x-cassandra-token", ASTRA_TOKEN)
				.POST(HttpRequest.BodyPublishers.ofString(data.toString()))
				.build();

		HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

		System.out.println(response.statusCode());
		System.out.println(response.body());
	}
}