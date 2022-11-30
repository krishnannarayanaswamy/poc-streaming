package io.section6.transaction.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/*
//      "id": "000010C3-409E-F0B7-63CB-FF76955514F9",
//	  "accountNumber": "999999173365949",
//	  "transactionDate": "2022-07-02 23:47:08",
//	  "processedDate": "2022-07-16 23:47:08",
//	  "amount": 900,
//	  "particulars": "9part",
//	  "code": "SampleCode",
//	  "reference": "online",
//	  "description": "transferfriend",
//	  "status": "Completed",
//	  "category": "Personal"
}
 */

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