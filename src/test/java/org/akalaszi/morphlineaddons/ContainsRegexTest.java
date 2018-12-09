package org.akalaszi.morphlineaddons;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;

public class ContainsRegexTest {

	@Test
	public void shouldKeepTheRecordIfRegexpDoesNotMatch() throws IOException {
		// GIVEN
		Command command = ConditionalDropTest.createMorphlineCommand("src/test/resources/containsRegex.conf");
		Record record = new Record();
		record.put("id", "bla2");

		// WHEN
		ConditionalDropTest.run(command, record);

		// THEN
		final String contains = record.get("contains").get(0).toString();
		Assert.assertTrue(Boolean.parseBoolean(contains));
	}
	
	@Test
	public void shouldDropTheRecordIfRegexpDoesNotMatch() throws IOException {
		// GIVEN
		Command command = ConditionalDropTest.createMorphlineCommand("src/test/resources/containsRegex.conf");
		Record record = new Record();
		record.put("id", "bla3");
		
		// WHEN
		ConditionalDropTest.run(command, record);
		
		// THEN
		final String contains = record.get("contains").get(0).toString();
		Assert.assertFalse(Boolean.parseBoolean(contains));
	}

}
