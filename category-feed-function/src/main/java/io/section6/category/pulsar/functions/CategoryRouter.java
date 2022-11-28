package io.section6.category.pulsar.functions;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import io.section6.category.model.CategoryData;

public class CategoryRouter implements Function<CategoryData, Void> {
	private String toEnSentimentrTopic, toDbTopic;
	private Schema schema = Schema.AVRO(CategoryData.class);

	public Void process(CategoryData categoryData, Context ctx) throws Exception {

		String lang = categoryData.getLang();
		System.out.println("############ tweetData.getLang(): " + lang);
		if (lang.compareTo("en") == 0) {
			ctx.newOutputMessage(ctx.getTenant() + "/"+ ctx.getNamespace()+ "/to-en-sentimentr", schema).properties(ctx.getCurrentRecord().getProperties())
					.value((CategoryData) categoryData).sendAsync();
		} else {
			ctx.newOutputMessage(ctx.getTenant() + "/"+ ctx.getNamespace()+ "/to-db", schema).properties(ctx.getCurrentRecord().getProperties())
					.value((CategoryData) categoryData).sendAsync();
		}

		return null;
	}
}