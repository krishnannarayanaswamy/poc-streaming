package io.section6.transaction.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TransactionData {
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
}