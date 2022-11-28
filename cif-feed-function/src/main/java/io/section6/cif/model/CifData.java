package io.section6.cif.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@Setter
public class CifData {
	public String tokenisedCif;
	public AccountRelationship[] accountRelationship;

	public Set<AccountWithCif> asAccountWithCif() {
		return Arrays.stream(this.accountRelationship).map((r) -> {
			AccountWithCif awc = new AccountWithCif();
			awc.accountnumber = r.accountNumber;
			awc.accounttype = r.type;
			awc.cifid = this.tokenisedCif;
			return awc;
		}).collect(Collectors.toSet());
	}
}

