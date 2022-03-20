/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.nosql.mongodb.resourceprovider.impl;

import org.apache.sling.api.resource.ResourceProviderFactory;
import org.apache.sling.nosql.generic.adapter.MetricsNoSqlAdapterWrapper;
import org.apache.sling.nosql.generic.adapter.NoSqlAdapter;
import org.apache.sling.nosql.generic.resource.AbstractNoSqlResourceProviderFactory;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.EventAdmin;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;

/**
 * {@link ResourceProviderFactory} implementation that uses MongoDB as persistence.
 */
@Component(immediate = true,
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        service = ResourceProviderFactory.class,
        name="org.apache.sling.nosql.mongodb.resourceprovider.MongoDBNoSqlResourceProviderFactory.factory.config",
        properties = {
                "webconsole.configurationFactory.nameHint=Root paths: {}"
        })
@Designate(ocd = MongoDBNoSqlResourceProviderFactory.Config.class, factory = true)
public final class MongoDBNoSqlResourceProviderFactory extends AbstractNoSqlResourceProviderFactory {

    @ObjectClassDefinition(name = "Apache Sling NoSQL MongoDB Resource Provider Factory",
            description = "Defines a resource provider factory with MongoDB persistence.")
    public @interface Config {

        @AttributeDefinition(name = "Root paths", description = "Root paths for resource provider.", cardinality = Integer.MAX_VALUE)
        String provider_roots() default "";

        @AttributeDefinition(name = "Connection String", description = "MongoDB connection String. Example: 'localhost:27017,localhost:27018,localhost:27019'")
        String connectionString() default MongoDBNoSqlResourceProviderFactory.CONNECTION_STRING_DEFAULT;

        @AttributeDefinition(name = "Database", description = "MongoDB database to store resource data in.")
        String database() default MongoDBNoSqlResourceProviderFactory.DATABASE_DEFAULT;

        @AttributeDefinition(name = "Collection", description = "MongoDB collection to store resource data in.")
        String collection() default MongoDBNoSqlResourceProviderFactory.COLLECTION_DEFAULT;
    }

    private static final String CONNECTION_STRING_DEFAULT = "localhost:27017";
    private static final String DATABASE_DEFAULT = "sling";
    private static final String COLLECTION_DEFAULT = "resources";
    
    @Reference
    private EventAdmin eventAdmin;

    private MongoClient mongoClient;
    private NoSqlAdapter noSqlAdapter;

    @Activate
    private void activate(ComponentContext componentContext, Config config) {
        String connectionString = config.connectionString();
        String database = config.database();
        String collection = config.collection();
        
        mongoClient = new MongoClient(connectionString);
        NoSqlAdapter mongodbAdapter = new MongoDBNoSqlAdapter(mongoClient, database, collection);
        
        // enable call logging and metrics for {@link MongoDBNoSqlAdapter}
        noSqlAdapter = new MetricsNoSqlAdapterWrapper(mongodbAdapter, LoggerFactory.getLogger(MongoDBNoSqlAdapter.class));
    }
    
    @Deactivate
    private void deactivate() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Override
    protected NoSqlAdapter getNoSqlAdapter() {
        return noSqlAdapter;
    }

    @Override
    protected EventAdmin getEventAdmin() {
        return eventAdmin;
    }

}
