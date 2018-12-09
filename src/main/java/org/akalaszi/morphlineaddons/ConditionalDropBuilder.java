/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.akalaszi.morphlineaddons;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

/**
 * Breaks the execution flow, when all of the given fields matches the provided
 * regex. A field contains a list in morphlines. If any members of the list
 * matches, the field matches.
 */
public final class ConditionalDropBuilder implements CommandBuilder {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConditionalDropBuilder.class);

	@Override
	public Collection<String> getNames() {
		return Collections.singletonList("conditionalDrop");
	}

	@Override
	public Command build(Config config, Command parent, Command child, MorphlineContext context) {
		return new DropRecord(this, config, parent, child, context);
	}

	private static final class DropRecord extends AbstractCommand {
		private Set<Map.Entry<String, ConfigValue>> entries;

		public DropRecord(CommandBuilder builder, Config config, Command parent, Command child,
				MorphlineContext context) {
			super(builder, config, parent, child, context);
			entries = config.entrySet();
		}

		@Override
		protected boolean doProcess(Record record) {

			try {
				int matchCount = 0;

				for (Entry<String, ConfigValue> e : entries) {
					final String key = e.getKey();
					final String regexp = dropQuotes(e.getValue().render());

					if (Fields.ATTACHMENT_BODY.equals(key)) {
						if (firstValueMatches(record, Fields.ATTACHMENT_BODY, regexp)) {
							matchCount++;
						}
					} else if (anyValueMatches(record, key, regexp)) {
						matchCount++;
					}

				}

				if (matchCount == entries.size()) {
					return true;
				}

				return super.doProcess(record);
			} catch (Exception e) {
				throw new MorphlineRuntimeException(e);
			}

		}

		private static boolean matches(Object object, String regexp) {
			if (object == null) {
				return false;
			}

			final String string = object.toString();
			final boolean matches = string.matches(regexp);
			LOGGER.debug("regex: >{}< value: >{}< {}", regexp, string, matches);
			return matches;
		}

		private static boolean firstValueMatches(Record record, String key, String regexp) {
			return matches(record.getFirstValue(key), regexp);
		}

		private static boolean anyValueMatches(Record record, String key, String regexp) {
			List<?> list = record.get(key);
			for (Object object : list) {
				if (matches(object, regexp)) {
					return true;
				}
			}
			return false;
		}

	}

	static String dropQuotes(final String render) {
		return render.substring(1, render.length() - 1);
	}

}