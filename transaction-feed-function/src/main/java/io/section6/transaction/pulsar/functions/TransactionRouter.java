package io.section6.transaction.pulsar.functions;

import io.section6.transaction.model.TransactionData;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.json.JSONObject;

public class TransactionRouter implements Function<TransactionData, Void> {
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

	public Void process(TransactionData transactionData, Context ctx) throws Exception {
		System.out.println(transactionData.id);

		HttpClient client = HttpClient.newHttpClient();

		this.insertTransactionsByAccount(client, transactionData);
		this.insertTransactionAccountWithCifView(client, transactionData);

		return null;
	}

	public List<String> getAccountCifs(HttpClient client, String accountNumber) throws Exception {
		String queryJson = "{ \"accountnumber\": { \"$eq\": \""+accountNumber+"\" } }";

		HttpRequest request = HttpRequest.newBuilder()
				.uri(this.appendUri(ENDPOINT + "/v2/keyspaces/transactions/account_with_cif", "where="+queryJson))
				.header("Content-Type", "application/json")
				.header("x-cassandra-token", ASTRA_TOKEN)
				.GET()
				.build();

		HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

		var json = new JSONObject(response.body());

		System.out.println("GetAccountWithCif");
		System.out.println(json.toString());

		List<String> cifs = new ArrayList<>();

		if (!json.has("data")) {
			return cifs;
		}

		var accountWithCifs = json.getJSONArray("data");

		for (int i = 0; i < accountWithCifs.length(); i++) {
			cifs.add(accountWithCifs.getJSONObject(i).getString("cifid"));
		}

		return cifs;
	}

	public void insertTransactionAccountWithCifView(HttpClient client, TransactionData transactionData) throws Exception {
		List<String> cifs = this.getAccountCifs(client, transactionData.accountNumber);

		JSONObject data = new JSONObject();

		data.put("accountnumber", transactionData.accountNumber);
		data.put("amount", transactionData.amount);
		data.put("category", transactionData.category);
		data.put("code", transactionData.code);
		data.put("description", transactionData.description);
		data.put("particular", transactionData.particulars);
		data.put("proccesseddate", transactionData.processedDate);
		data.put("reference", transactionData.reference);
		data.put("status", transactionData.status);
		data.put("transactiondate", transactionData.transactionDate);
		data.put("transactionid", transactionData.id);

		data.put("cifid", "{" + String.join(",", cifs).replace("\"", "'") + "}");

		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(ENDPOINT + "/v2/keyspaces/transactions/transactions_account_with_cif_view"))
				.header("Content-Type", "application/json")
				.header("x-cassandra-token", ASTRA_TOKEN)
				.POST(HttpRequest.BodyPublishers.ofString(data.toString()))
				.build();

		HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

		System.out.println(response.statusCode());
		System.out.println(response.body());
	}

	public void insertTransactionsByAccount(HttpClient client, TransactionData transactionData) throws Exception {
		JSONObject data = new JSONObject();

		data.put("accountnumber", transactionData.accountNumber);
		data.put("amount", transactionData.amount);
		data.put("category", transactionData.category);
		data.put("code", transactionData.code);
		data.put("description", transactionData.description);
		data.put("particular", transactionData.particulars);
		data.put("proccesseddate", transactionData.processedDate);
		data.put("reference", transactionData.reference);
		data.put("status", transactionData.status);
		data.put("transactiondate", transactionData.transactionDate);
		data.put("transactionid", transactionData.id);

		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(ENDPOINT + "/v2/keyspaces/transactions/transactions_by_account"))
				.header("Content-Type", "application/json")
				.header("x-cassandra-token", ASTRA_TOKEN)
				.POST(HttpRequest.BodyPublishers.ofString(data.toString()))
				.build();

		HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

		System.out.println(response.statusCode());
		System.out.println(response.body());
	}

	public URI appendUri(String uri, String appendQuery) throws URISyntaxException {
		URI oldUri = new URI(uri);

		String newQuery = oldUri.getQuery();
		if (newQuery == null) {
			newQuery = appendQuery;
		} else {
			newQuery += "&" + appendQuery;
		}

		return new URI(oldUri.getScheme(), oldUri.getAuthority(),
				oldUri.getPath(), newQuery, oldUri.getFragment());
	}
}