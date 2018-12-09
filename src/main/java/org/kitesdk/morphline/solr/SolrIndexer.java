/*
 * Copyright 2013 Cloudera Inc.
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
package org.kitesdk.morphline.solr;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.retry.MetricsFacade;
import org.apache.solr.client.solrj.retry.RetryPolicyFactory;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.api.TypedSettings;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Metrics;
import org.kitesdk.morphline.solr.DocumentLoader;
import org.kitesdk.morphline.solr.LoadSolrBuilder;
import org.kitesdk.morphline.solr.RateLimiter;
import org.kitesdk.morphline.solr.RetryPolicyFactoryParser;
import org.kitesdk.morphline.solr.SolrLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class SolrIndexer {
	private static final Logger LOGGER = LoggerFactory.getLogger(SolrIndexer.class);
	private static final boolean DISABLE_RETRY_POLICY_BY_DEFAULT = Boolean.parseBoolean(
			System.getProperty(LoadSolrBuilder.class.getName() + ".disableRetryPolicyByDefault", "false"));

	private final DocumentLoader loader;
	private final RateLimiter rateLimiter;
	private final Timer elapsedTime;
	private final boolean isDryRun;

	public SolrIndexer(Config config, MorphlineContext context, String collectionName) {

		Configs configs = new Configs();
		this.loader = initDocumentLoader(config, context, collectionName, configs);
		this.rateLimiter = initRateLimiter(config, configs);
		configs.validateArguments(config);

		this.isDryRun = initDryRun(context);
		this.elapsedTime = initTimer(context, collectionName, new String[] { Metrics.ELAPSED_TIME });
	}

	private Timer initTimer(MorphlineContext context, String collectionName, String[] names) {
		return context.getMetricRegistry().timer(MetricRegistry.name("LoadSolrDCXA-" + collectionName, names));
	}

	private boolean initDryRun(MorphlineContext context) {
		return context.getTypedSettings().getBoolean(TypedSettings.DRY_RUN_SETTING_NAME, false);
	}

	private static RateLimiter initRateLimiter(Config config, Configs configs) {
		return RateLimiter.create(configs.getDouble(config, "maxRecordsPerSecond", Double.MAX_VALUE));
	}

	private static DocumentLoader initDocumentLoader(Config config, MorphlineContext context, String collectionName,
			Configs configs) {
		Config solrLocatorConfig = configs.getConfig(config, LoadSolrBuilder.SOLR_LOCATOR_PARAM);
		SolrLocator locator = new SolrLocator(solrLocatorConfig, context);
		locator.setCollectionName(collectionName);
		LOGGER.debug("solrLocator: {}", locator);

		RetryPolicyFactory retryPolicyFactory = parseRetryPolicyFactory(configs.getConfig(config, "retryPolicy", null));
		return locator.getLoader(retryPolicyFactory, new CodahaleMetricsFacade(context.getMetricRegistry()));
	}

	private static RetryPolicyFactory parseRetryPolicyFactory(Config retryPolicyConfig) {
		if (retryPolicyConfig == null && !DISABLE_RETRY_POLICY_BY_DEFAULT) {
			// ask RetryPolicyFactoryParser to return a retry policy with
			// reasonable defaults
			retryPolicyConfig = ConfigFactory
					.parseString("{" + RetryPolicyFactoryParser.BOUNDED_EXPONENTIAL_BACKOFF_RETRY_NAME + "{}}");
		}
		if (retryPolicyConfig == null) {
			return null;
		} else {
			return new RetryPolicyFactoryParser().parse(retryPolicyConfig);
		}
	}

	public void beginTransaction() {
		try {
			loader.beginTransaction();
		} catch (IOException | SolrServerException e) {
			throw new MorphlineRuntimeException(e);
		}
	}

	public void commitTransaction() {
		try {
			loader.commitTransaction();
		} catch (IOException | SolrServerException e) {
			throw new MorphlineRuntimeException(e);
		}
	}

	public void rollbackTransaction() {
		try {
			loader.rollbackTransaction();
		} catch (IOException | SolrServerException e) {
			throw new MorphlineRuntimeException(e);
		}
	}

	public void shutdown() {
		try {
			loader.shutdown();
		} catch (IOException | SolrServerException e) {
			throw new MorphlineRuntimeException(e);
		}
	}

	public void sendToSolr(Record record) {
		rateLimiter.acquire();
		Timer.Context timerContext = elapsedTime.time();
		List<?> deleteById = record.get(LoadSolrBuilder.LOAD_SOLR_DELETE_BY_ID);
		List<?> deleteByQuery = record.get(LoadSolrBuilder.LOAD_SOLR_DELETE_BY_QUERY);
		try {
			if (deleteById.isEmpty() && deleteByQuery.isEmpty()) {
				saveRecord(record);
				return;
			} 
			deleteByIds(deleteById);
			deleteByQuery(deleteByQuery);
		} catch (IOException | SolrServerException e) {
			throw new MorphlineRuntimeException(e);
		} finally {
			timerContext.stop();
		}

	}

	private void saveRecord(Record record) throws IOException, SolrServerException {
		SolrInputDocument doc = convert(record);
		if (isDryRun) {
			LOGGER.info("dryrun: update: {}", doc);
		} else {
			loader.load(doc);
		}
	}

	private void deleteByQuery(List<?> deleteByQuery) throws IOException, SolrServerException {
		for (Object query : deleteByQuery) {
			if (isDryRun) {
				LOGGER.info("dryrun: deleteByQuery: {}", query);
			} else {
				loader.deleteByQuery(query.toString());
			}
		}
	}

	private void deleteByIds(List<?> deleteById) throws IOException, SolrServerException {
		for (Object id : deleteById) {
			if (isDryRun) {
				LOGGER.info("dryrun: deleteById: {}", id);
			} else {
				loader.deleteById(id.toString());
			}
		}
	}

	private SolrInputDocument convert(Record record) {
		Map<String, Collection<Object>> map = record.getFields().asMap();
		SolrInputDocument doc = new SolrInputDocument(new HashMap<String, SolrInputField>(2 * map.size()));
		for (Map.Entry<String, Collection<Object>> entry : map.entrySet()) {
			String key = entry.getKey();
			if (LoadSolrBuilder.LOAD_SOLR_CHILD_DOCUMENTS.equals(key)) {
				for (Object value : entry.getValue()) {
					if (value instanceof Record) {
						value = convert((Record) value); // recurse
					}
					if (value instanceof SolrInputDocument) {
						doc.addChildDocument((SolrInputDocument) value);
					} else {
						throw new MorphlineRuntimeException("Child document must be of class " + Record.class.getName()
								+ " or " + SolrInputDocument.class.getName() + ": " + value);
					}
				}
			} else {
				Collection<Object> values = entry.getValue();
				if (values.size() == 1 && values.iterator().next() instanceof Map) {
					doc.setField(key, values.iterator().next()); // it is an
																	// atomic
																	// update
				} else {
					doc.setField(key, values);
				}
			}
		}
		return doc;
	}

	/**
	 * A facade using codahale metrics as a backend.
	 */
	private static final class CodahaleMetricsFacade implements MetricsFacade {

		private final MetricRegistry registry;

		public CodahaleMetricsFacade(MetricRegistry registry) {
			Preconditions.checkNotNull(registry);
			this.registry = registry;
		}

		@Override
		public void markMeter(String name, long increment) {
			registry.meter(name).mark(increment);
		}

		@Override
		public void updateHistogram(String name, long value) {
			registry.histogram(name).update(value);
		}

		@Override
		public void updateTimer(String name, long duration, TimeUnit unit) {
			registry.timer(name).update(duration, unit);
		}

	}

}