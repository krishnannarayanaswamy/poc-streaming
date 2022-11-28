package io.section6.category.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CategoryData {
	public String lang;
    public String id;
	public String tweet;
	public String createdAt;
	public int sentiment;
}
