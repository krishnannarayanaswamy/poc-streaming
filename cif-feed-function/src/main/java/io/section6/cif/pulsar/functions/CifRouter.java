package io.section6.cif.pulsar.functions;

import io.section6.cif.model.AccountWithCif;
import io.section6.cif.model.CifData;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class CifRouter implements Function<CifData, Void> {
	private static final String ASTRA_TOKEN = "AstraCS:RgUfqIQuoDmWoKbIpMDDTTMR:41aba37ec0891bb201c3c81ec66fe4719c744e285936ec6dc9a0b05f2ef08972";
	private static final String ENDPOINT = "https://1e4f081a-c9de-4b95-9607-1ffb6b202b60-australiaeast.apps.astra.datastax.com/api/rest";
	private static final String CIF_END = "/v2/keyspaces/transactions/account_with_cif";

	public Void process(CifData cifData, Context ctx) throws Exception {
		System.out.println(cifData.tokenisedCif);

		HttpClient client = HttpClient.newHttpClient();

		for (AccountWithCif awc: cifData.asAccountWithCif()) {
			// work-around to not include jackson serialization lib
			String json = "{ \"cifid\": \""+ awc.cifid +"\", \"accountnumber\": \""+awc.accountnumber+"\", \"accounttype\": \""+awc.accounttype+"\"}";

			var request = HttpRequest.newBuilder()
					.uri(URI.create(ENDPOINT + CIF_END))
					.header("Content-Type", "application/json")
					.header("x-cassandra-token", ASTRA_TOKEN)
					.POST(HttpRequest.BodyPublishers.ofString(json))
					.build();

			var response = client.send(request, HttpResponse.BodyHandlers.ofString());

			System.out.println(response.statusCode());
			System.out.println(response.body());
		}

		/*

		additional topic + sink method

		String outputTopic = ctx.getTenant() + "/" + ctx.getNamespace() + "/cif-data-to-db";

		for (AccountWithCif awc: cifData.asAccountWithCif()) {
			ctx.newOutputMessage(outputTopic, outputSchema)
					.properties(ctx.getCurrentRecord().getProperties())
					.value((AccountWithCif) awc)
					.sendAsync();
		}

		*/

		return null;

	}
}