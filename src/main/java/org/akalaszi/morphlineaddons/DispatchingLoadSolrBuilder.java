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
import java.util.HashMap;
import java.util.Map;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Notifications;
import org.kitesdk.morphline.solr.SolrIndexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

public final class DispatchingLoadSolrBuilder implements CommandBuilder {
	private static final Logger LOGGER = LoggerFactory.getLogger(DispatchingLoadSolrBuilder.class);
	private static final String TARGET_COLLECTION_PROPERTY_NAME = "targetCollection";

	@Override
	public Collection<String> getNames() {
		return Collections.singletonList("dispatchingLoadSolr");
	}

	@Override
	public Command build(Config config, Command parent, Command child, MorphlineContext context) {
		return new DispatchingLoadSolr(this, config, parent, child, context);
	}

	private static final class DispatchingLoadSolr extends AbstractCommand {
		private final ThreadLocal<Map<String, SolrIndexer>> threadLocal = ThreadLocal.withInitial(HashMap::new);
		
		public DispatchingLoadSolr(CommandBuilder builder, Config config, Command parent, Command child,
				MorphlineContext context) {
			super(builder, config, parent, child, context);
		}

		@Override
		protected boolean doProcess(Record record) {
//			LOGGER.info("----- doProcess Threadname: {}", Thread.currentThread().getName());
			String targetCollection = (String) record.getFirstValue(TARGET_COLLECTION_PROPERTY_NAME);
			if (targetCollection == null) {
				LOGGER.warn("Property targetCollection has not been defined, omitting this record: {}", record);
			} else {
				final SolrIndexer loader = obtainSolrIndexer(targetCollection);
				LOGGER.debug("Sending to collection: {} Record: {} ", targetCollection, record);
				Record r = record.copy();
				r.removeAll(TARGET_COLLECTION_PROPERTY_NAME);
				loader.sendToSolr(r);
			}

			return super.doProcess(record);
		}

		private synchronized SolrIndexer obtainSolrIndexer(String targetCollection) {
			final Map<String, SolrIndexer> localMap = localMap();
			if (localMap.containsKey(targetCollection)) {
				return localMap.get(targetCollection);
			}
			localMap.forEach((k, v) -> {
				LOGGER.info("commit & shutdown for {} ", k);
				v.commitTransaction(); 
				v.shutdown();
			});
			localMap.clear();
			
			SolrIndexer loader = new SolrIndexer(super.getConfig(), super.getContext(), targetCollection);
			loader.beginTransaction();
			localMap.put(targetCollection, loader);
			return loader;
		}

		private Map<String, SolrIndexer> localMap() {
			return threadLocal.get();
		}

		@Override
		protected void doNotify(Record notification) {
//			LOGGER.info("----- doNotify Threadname: {}", Thread.currentThread().getName());

			for (Object event : Notifications.getLifecycleEvents(notification)) {
			
				if ( Notifications.LifecycleEvent.COMMIT_TRANSACTION.equals(event)) {
					localMap().forEach((k, v) -> { 
						LOGGER.debug("COMMIT_TRANSACTION for {} ", k);
						v.commitTransaction(); 
						});
				} 
				
				if (event == Notifications.LifecycleEvent.ROLLBACK_TRANSACTION) {
					localMap().forEach((k, v) -> {
						LOGGER.debug("ROLLBACK_TRANSACTION for {} ", k);
						v.rollbackTransaction();
						});
				} 
				
				if (event == Notifications.LifecycleEvent.SHUTDOWN) {
					localMap().forEach((k, v) -> {
						LOGGER.debug("SHUTDOWN_TRANSACTION for {} ", k);
						v.shutdown();
					});
				}
				
			}
			super.doNotify(notification);
		}

	}

}