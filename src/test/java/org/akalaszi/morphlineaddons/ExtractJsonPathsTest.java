package org.akalaszi.morphlineaddons;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;

public class ExtractJsonPathsTest {

	@Test
	public void shouldExtractJsonfields() throws IOException {
		// GIVEN
		Command command = ConditionalDropTest.createMorphlineCommand("src/test/resources/extract.conf");
		Record record = createRecord();

		// WHEN
		ConditionalDropTest.run(command, record);

		// THEN
		// new record is created that should contain the json path values per field.
	}

	private Record createRecord() throws IOException {
		Path path = Paths.get("src/test/resources/lithium_record_noteaser.json");
		String data = new String(Files.readAllBytes(path));
		Record record = new Record();
		record.put("fullcontent", data);
		return record;
	}

}
