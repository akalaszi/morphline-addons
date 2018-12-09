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

import org.jsoup.Jsoup;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

public final class StripHtmlTagsBuilder implements CommandBuilder {

	@Override
	public Collection<String> getNames() {
		return Collections.singletonList("stripHtmlTags");
	}

	@Override
	public Command build(Config config, Command parent, Command child, MorphlineContext context) {
		return new StripHtmlTags(this, config, parent, child, context);
	}

	private static final class StripHtmlTags extends AbstractCommand {

		private final String fieldsToStrip;

		public StripHtmlTags(CommandBuilder builder, Config config, Command parent, Command child,
				MorphlineContext context) {
			super(builder, config, parent, child, context);
			fieldsToStrip = getConfigs().getString(config, "fieldsToStrip");
		}

		@Override
		protected boolean doProcess(Record record) {
			try {
				String[] fields = fieldsToStrip.split(",");
				for (String f : fields) {
					String content = HtmlProcessorBuilder.parseAsString(record.getFirstValue(f));
					record.replaceValues(f, Jsoup.parse(content).text());
				}
				return super.doProcess(record);
			} catch (Exception e) {
				throw new MorphlineRuntimeException(e);
			}
		}
	}
}
