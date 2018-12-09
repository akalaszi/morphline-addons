package org.akalaszi.morphlineaddons;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Notifications;

public class ConditionalDropTest {

	@Test
	public void shouldKeepTheRecordIfRegexpMatches() throws IOException {
		// GIVEN
		Command command = createMorphlineCommand("src/test/resources/drop.conf");
		Record record = new Record();
		record.put("archieve", "topics/cm_props_cdh5100_javakeystorekms.html");

		// WHEN
		run(command, record);

		// THEN
		@SuppressWarnings("rawtypes")
		List l = record.get("archieve");
		Assert.assertEquals("topics/cm_props_cdh5100_javakeystorekms.html", l.get(0).toString());
		Assert.assertEquals(1, record.get("continued").size());
	}

	@Test
	public void shouldDropTheRecordIfRegexpDoesNotMatch() throws IOException {
		// GIVEN
		Command command = createMorphlineCommand("src/test/resources/drop.conf");
		Record record = new Record();
		record.put("archieve", "topics/cm_props_cdh5100_javakeystorekms.jpg");

		// WHEN
		run(command, record);

		// THEN
		Assert.assertTrue(record.get("continued").isEmpty());
	}

	@Test
	public void shouldNotDropOnPartialMatches() throws IOException {
		// GIVEN
		Command command = createMorphlineCommand("src/test/resources/dropMultiple.conf");
		Record record = new Record();
		record.put("boardId", "DevBoard");
		record.put("depth", "1");

		// WHEN
		run(command, record);

		// THEN
		Assert.assertEquals(1, record.get("continued").size());
	}

	@Test
	public void shouldDropOnEveryFieldMatches() throws IOException {
		// GIVEN
		Command command = createMorphlineCommand("src/test/resources/dropMultiple.conf");
		Record record = new Record();
		record.put("boardId", "customer-tkb");
		record.put("depth", "1");

		// WHEN
		run(command, record);

		// THEN
		Assert.assertTrue(record.get("continued").isEmpty());
	}

	static void run(Command command, Record record) {
		Notifications.notifyStartSession(command);
		boolean success = command.process(record);
		if (!success) {
			Assert.fail("Morphline failed to process record:" + record);
		}
		Notifications.notifyCommitTransaction(command);
		Notifications.notifyShutdown(command);
	}

	static Command createMorphlineCommand(final String config) {
		MorphlineContext morphlineContext = new MorphlineContext.Builder().build();
		Command morphline = new Compiler().compile(new File(config), null, morphlineContext, null);
		Notifications.notifyBeginTransaction(morphline);
		return morphline;
	}

}
