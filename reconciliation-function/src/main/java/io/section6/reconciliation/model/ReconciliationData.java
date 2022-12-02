package io.section6.reconciliation.model;

import lombok.Getter;
import lombok.Setter;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
public class ReconciliationData {
	public String id;
	public String accountNumber;
	public String transactionDate;
	public String processedDate;
	public int amount;
	public String particulars;
	public String code;
	public String reference;
	public String description;
	public String status;
	public String category;

	public Set<String> compareWithTransaction(JSONObject transaction) {
		Set<String> differences = new HashSet<>();

		if (!transaction.getString("transactiondate").equals(this.transactionDate)) {
			differences.add("transactiondate");
		}

		if (!transaction.getString("proccesseddate").equals(this.processedDate)) {
			differences.add("proccesseddate");
		}

		if (transaction.getDouble("amount") != this.amount) {
			differences.add("amount");
		}

		if (!transaction.getString("particular").equals(this.particulars)) {
			differences.add("particulars");
		}

		if (!transaction.getString("code").equals(this.code)) {
			differences.add("code");
		}

		if (!transaction.getString("reference").equals(this.reference)) {
			differences.add("reference");
		}

		if (!transaction.getString("description").equals(this.description)) {
			differences.add("description");
		}

		if (!transaction.getString("status").equals(this.status)) {
			differences.add("status");
		}

		return differences;
	}
}
