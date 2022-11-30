package io.section6.cif.pulsar.functions;

import io.section6.cif.model.AccountWithCif;
import io.section6.cif.model.CifData;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

public class CifRouter implements Function<CifData, Void> {
	private String ASTRA_TOKEN = "<replace-me>";
	private static final String ENDPOINT = "https://1e4f081a-c9de-4b95-9607-1ffb6b202b60-australiaeast.apps.astra.datastax.com/api/rest";

//	@Override
//	public void initialize(Context context) {
//
//		System.out.println(context.getUserConfigMap());
//
//		Optional<Object> token = context.getUserConfigValue("astra-token");
//
//		if (token.isEmpty()) {
//			throw new RuntimeException("No astra-token found");
//		}
//
//		this.ASTRA_TOKEN = token.toString();
//	}

	public Void process(CifData cifData, Context ctx) throws Exception {
		System.out.println(cifData.tokenisedCif);

		HttpClient client = HttpClient.newHttpClient();

		Set<AccountWithCif> accountWithCifs = cifData.asAccountWithCif();

		this.insertAccountWithCif(client, accountWithCifs);
		this.checkTransactionAccountWithCifView(client, accountWithCifs);

		return null;
	}

	private void insertAccountWithCif(HttpClient client, Set<AccountWithCif> awcs) throws Exception {
		for (AccountWithCif awc: awcs) {
			HttpRequest request = HttpRequest.newBuilder()
					.uri(URI.create(ENDPOINT + "/v2/keyspaces/transactions/account_with_cif"))
					.header("Content-Type", "application/json")
					.header("x-cassandra-token", ASTRA_TOKEN)
					.POST(HttpRequest.BodyPublishers.ofString(new JSONObject(awc).toString()))
					.build();

			HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

			System.out.println("AccountWithCif");
			System.out.println(response.statusCode());
			System.out.println(response.body());
		}
	}

	private void checkTransactionAccountWithCifView(HttpClient client, Set<AccountWithCif> awcs) throws Exception {

		for (AccountWithCif awc: awcs) {
			String queryJson = "{ \"accountnumber\": { \"$eq\": \""+awc.accountnumber+"\" } }";

			HttpRequest request = HttpRequest.newBuilder()
					.uri(this.appendUri(ENDPOINT + "/v2/keyspaces/transactions/transactions_account_with_cif_view", "where="+queryJson))
					.header("Content-Type", "application/json")
					.header("x-cassandra-token", ASTRA_TOKEN)
					.GET()
					.build();

			HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

			JSONObject json = new JSONObject(response.body());

			// only update if existing account exists
			if (json.getInt("count") > 0) {
				System.out.println("Transaction account found");

				JSONArray cifids = json.getJSONArray("data").getJSONObject(0).getJSONArray("cifid");

				// merge in new cif-id
				cifids.put(awc.cifid);

				this.updateCifOnTransactionAccountWithCifView(client, awc.accountnumber, cifids);

				return;
			}

			System.out.println("No transaction account found, skipping");
		}
	}

	private void updateCifOnTransactionAccountWithCifView(HttpClient client, String primaryKey, JSONArray cifIds) throws IOException, InterruptedException {
		System.out.println(String.format("Updating view table for account: %s", primaryKey));

		HashMap<String, String> data = new HashMap();

		data.put("cifid", "{" + cifIds.join(",").replace("\"", "'") + "}");

		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(ENDPOINT + "/v2/keyspaces/transactions/transactions_account_with_cif_view/"+primaryKey))
				.header("Content-Type", "application/json")
				.header("x-cassandra-token", ASTRA_TOKEN)
				.method("PATCH", HttpRequest.BodyPublishers.ofString(new JSONObject(data).toString()))
				.build();

		HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

		System.out.println("Transaction");
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