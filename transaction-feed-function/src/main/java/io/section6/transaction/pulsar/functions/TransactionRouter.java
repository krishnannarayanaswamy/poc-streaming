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

import org.json.JSONObject;

public class TransactionRouter implements Function<TransactionData, Void> {
	private static final String ASTRA_TOKEN = "AstraCS:RgUfqIQuoDmWoKbIpMDDTTMR:41aba37ec0891bb201c3c81ec66fe4719c744e285936ec6dc9a0b05f2ef08972";
	private static final String ENDPOINT = "https://1e4f081a-c9de-4b95-9607-1ffb6b202b60-australiaeast.apps.astra.datastax.com/api/rest";

	public Void process(TransactionData transactionData, Context ctx) throws Exception {
		System.out.println(transactionData.id);

		HttpClient client = HttpClient.newHttpClient();

		this.insertTransaction(client, transactionData);

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

		List<String> cifs = new ArrayList<>();

		var accountWithCifs = json.getJSONArray("data");

		for (int i = 0; i < accountWithCifs.length(); i++) {
			cifs.add(accountWithCifs.getJSONObject(i).getString("cifid"));
		}

		return cifs;
	}

	public void insertTransaction(HttpClient client, TransactionData transactionData) throws Exception {
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

		System.out.println("Insert");
		System.out.println(data);


		 /*
		     accountnumber text,
    accounttype text,
    amount decimal,
    category text,
    cifid SET<text> static,
    code text,
    description text,
    particular text,
    proccesseddate timestamp,
    reference text,
    status text,
    transactiondate timestamp,
    transactionid text,
		  */



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