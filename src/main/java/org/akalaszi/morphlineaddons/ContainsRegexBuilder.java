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
import java.util.Set;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

public class ContainsRegexBuilder implements CommandBuilder {

	@Override
	public Collection<String> getNames() {
		return Collections.singletonList("containsRegex");
	}

	@Override
	public Command build(Config config, Command parent, Command child, MorphlineContext context) {
		return new ContainsRegex(this, config, parent, child, context);
	}

	private static final class ContainsRegex extends AbstractCommand {

		private Set<Map.Entry<String, ConfigValue>> entries;

		public ContainsRegex(CommandBuilder builder, Config config, Command parent, Command child,
				MorphlineContext context) {
			super(builder, config, parent, child, context);
			this.entries = config.entrySet();
		}

		@Override
		protected boolean doProcess(Record record) {
			for (Map.Entry<String, ConfigValue> entry : entries) {
				String fieldName = entry.getKey();
				String regexString = ConditionalDropBuilder.dropQuotes(entry.getValue().render());

				List<?> recordValues = record.get(fieldName);
				
				boolean found = false;
				for (Object value : recordValues) {
					if (value != null && value.toString().matches(regexString)) {
						found = true;
						break;
					}
				}

				if (!found) {
					return false;
				}
			}
			return super.doProcess(record);
		}

	}

}
