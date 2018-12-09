package org.akalaszi.morphlineaddons;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;

public class HtmlProcessorTest {

	@Test
	public void shouldGetHtmlElement() throws IOException {
		// GIVEN
		Command command = ConditionalDropTest.createMorphlineCommand("src/test/resources/htmlprocessor.conf");
		Record record = createRecord(Fields.ATTACHMENT_BODY, "src/test/resources/test.html");

		// WHEN
		ConditionalDropTest.run(command, record);

		// THEN
		@SuppressWarnings({ "rawtypes", "unchecked" })
		final Map<String, String> title = (Map) record.get("title").get(0);

		Assert.assertEquals("title", title.get("set"));
	}

	@Test
	public void shouldSurviveBinaryFile() throws Exception {

		// GIVEN
		Command command = ConditionalDropTest.createMorphlineCommand("src/test/resources/htmlprocessor.conf");
		Record record = createRecordByteArray(Fields.ATTACHMENT_BODY, "src/test/resources/img.png");

		// WHEN
		ConditionalDropTest.run(command, record);

		// THEN
		Assert.assertTrue(record.get("title").isEmpty());
	}

	@Test
	public void shouldBeAbleToCaptureXmlAttributeValues() throws Exception {
		// GIVEN
		Command command = ConditionalDropTest.createMorphlineCommand("src/test/resources/htmlprocessor2.conf");
		Record record = createRecordByteArray("source", "src/test/resources/test2.html");

		// WHEN
		ConditionalDropTest.run(command, record);

		// THEN
		@SuppressWarnings({ "rawtypes", "unchecked" })
		final Map<String, List> facets = (Map) record.get("content_facets").get(0);

		Assert.assertEquals(Arrays.asList("a", "b", "c"), facets.get("set"));
	}

	private Record createRecord(final String key, final String pathname) throws FileNotFoundException {
		File inputRecord = new File(pathname);
		InputStream input = new BufferedInputStream(new FileInputStream(inputRecord));
		Record record = new Record();
		record.put(key, input);
		return record;
	}

	private Record createRecordByteArray(final String key, final String pathname) throws Exception {
		byte[] data = Files.readAllBytes(Paths.get(pathname));
		Record record = new Record();
		record.put(key, data);
		return record;
	}

}
