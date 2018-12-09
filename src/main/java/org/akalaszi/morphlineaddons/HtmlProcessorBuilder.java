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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;

import com.google.common.io.ByteStreams;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigValue;

import com.google.common.base.Optional;

public final class HtmlProcessorBuilder implements CommandBuilder {

	@Override
	public Collection<String> getNames() {
		return Collections.singletonList("htmlProcessor");
	}

	@Override
	public Command build(Config config, Command parent, Command child, MorphlineContext context) {
		return new HtmlProcessor(this, config, parent, child, context);
	}

	private static final class HtmlProcessor extends AbstractCommand {

		private static final String SOURCE_FIELD_NAME = "_source";

		private final Set<Map.Entry<String, ConfigValue>> entries;
		private final String sourceFieldName;

		public HtmlProcessor(CommandBuilder builder, Config config, Command parent, Command child,
				MorphlineContext context) {
			super(builder, config, parent, child, context);
			final Set<Entry<String, ConfigValue>> entrySet = config.entrySet();
			this.entries = entrySet.stream().filter(e -> !SOURCE_FIELD_NAME.equals(e.getKey().replaceAll("\"", "")))
					.collect(Collectors.toSet());
			this.sourceFieldName = getConfigOrDefault(config, SOURCE_FIELD_NAME, Fields.ATTACHMENT_BODY);
		}

		private String getConfigOrDefault(Config config, String key, String defaultValue) {
			try {
				return config.getString(key);
			} catch (ConfigException.Missing e) {
				return defaultValue;
			}
		}

		@Override
		protected boolean doProcess(Record record) {
			final Object firstValue = record.getFirstValue(sourceFieldName);
			try {
				String content = parseAsString(firstValue);
				Document doc = Jsoup.parse(content);
				for (Entry<String, ConfigValue> e : entries) {
					final String cssSelector = e.getValue().render().replaceAll("\"", "");

					final Optional<Map<String, ?>> extracted = extract(doc, cssSelector);
					if (extracted.isPresent()) {
						record.put(e.getKey(), extracted.get());
					} else {
						return true;
					}
				}

				return super.doProcess(record);

			} catch (Exception e) {
				throw new MorphlineRuntimeException(e);
			}
		}

		/**
		 * 
		 * @param doc
		 * @param css
		 *            optionally, after the ; you can specify the attribute
		 *            name. If there is no attribute name specified, the dom
		 *            element is returned.
		 * @return
		 */
		static Optional<Map<String, ?>> extract(Document doc, String css) {
			String[] selectors = css.split(";");
			List<String> ret = new ArrayList<>();
			final Elements elements = doc.select(selectors[0]);
			for (Element e : elements) {
				final String content;
				if (selectors.length == 2) {
					content = e.attr(selectors[1]);
				} else {
					content = Jsoup.parse(e.toString()).text();
				}
				ret.add(content);
			}
			if (ret.isEmpty()) {
				return Optional.absent();
			}
			if (ret.size() == 1) {
				return Optional.of(Collections.singletonMap("set", ret.get(0)));
			}
			return Optional.of(Collections.singletonMap("set", ret));
		}

	}

	public static String parseAsString(Object s) throws IOException {
		if (s instanceof String) {
			return (String) s;
		}

		if (s instanceof byte[]) {
			return new String((byte[]) s);
		}

		if (s instanceof InputStream) {
			return new String(ByteStreams.toByteArray((InputStream) s));
		}
		return "";
	}

}
