package test.elasticsearch.plugin.river.mongodb;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.deleteIndexRequest;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPerparer;
import org.elasticsearch.plugins.PluginManager;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.process.distribution.GenericVersion;
import de.flapdoodle.embed.process.runtime.Network;

@Test
public abstract class RiverMongoDBTestAsbtract {

	private final ESLogger logger = Loggers.getLogger(getClass());

    private static final String MAPPER_ATTACHMENT_PLUGIN_NAME = "elasticsearch/elasticsearch-mapper-attachments/1.6.0";
    private static boolean USE_DYNAMIC_PORTS = false;
    //Version.Main.V2_2
    private static final String MONGO_VERSION = "2.0.4";
	public static final String ADMIN_DATABASE_NAME = "admin";
	public static final String LOCAL_DATABASE_NAME = "local";
	public static final String REPLICA_SET_NAME = "rep1";
	public static final String OPLOG_COLLECTION = "oplog.rs";

	private MongodConfig mongodConfig1;
	private MongodConfig mongodConfig2;
	private MongodConfig mongodConfig3;
	private MongodExecutable mongodExe1;
	private static int mongoPort1;
	private static MongodProcess mongod1;
	private MongodExecutable mongodExe2;
	private static int mongoPort2;
	private static MongodProcess mongod2;
	private MongodExecutable mongodExe3;
	private static int mongoPort3;
	private static MongodProcess mongod3;
	private static Mongo mongo;
	private DB mongoAdminDB;

	private static Node node;

	@BeforeSuite
	public void beforeSuite() throws Exception {
		logger.debug("*** beforeSuite ***");
		if (USE_DYNAMIC_PORTS) {
			mongoPort1 = Network.getFreeServerPort();
			mongoPort2 = Network.getFreeServerPort();
			mongoPort3 = Network.getFreeServerPort();
		} else {
			mongoPort1 = 37017;
			mongoPort2 = 37018;
			mongoPort3 = 37019;
		}
		setupElasticsearchServer();
		initMongoInstances();
	}

	private void initMongoInstances() throws Exception {
		logger.debug("*** initMongoInstances ***");
		CommandResult cr;
		
		// Create 3 mongod processes
		mongodConfig1 = new MongodConfig(new GenericVersion(MONGO_VERSION), null,
				mongoPort1, Network.localhostIsIPv6(), null,
				REPLICA_SET_NAME, 20);
		MongodStarter starter = MongodStarter.getDefaultInstance();
		mongodExe1 = starter.prepare(mongodConfig1);
		mongod1 = mongodExe1.start();
		mongodConfig2 = new MongodConfig(new GenericVersion(MONGO_VERSION), null,
				mongoPort2, Network.localhostIsIPv6(), null,
				REPLICA_SET_NAME, 20);
		mongodExe2 = starter.prepare(mongodConfig2);
		mongod2 = mongodExe2.start();
		mongodConfig3 = new MongodConfig(new GenericVersion(MONGO_VERSION), null,
				mongoPort3, Network.localhostIsIPv6(), null,
				REPLICA_SET_NAME, 20);
		mongodExe3 = starter.prepare(mongodConfig3);
		mongod3 = mongodExe3.start();
		String server1 = Network.getLocalHost().getHostName() + ":"
				+ mongodConfig1.getPort();
		String server2 = Network.getLocalHost().getHostName() + ":"
				+ mongodConfig2.getPort();
		String server3 = Network.getLocalHost().getHostName() + ":"
				+ mongodConfig3.getPort();
		logger.debug("Server #1: {}", server1);
		logger.debug("Server #2: {}", server2);
		logger.debug("Server #3: {}", server3);
		Thread.sleep(2000);
		MongoOptions mo = new MongoOptions();
		mo.autoConnectRetry = true;
		mongo = new Mongo(new ServerAddress(Network.getLocalHost().getHostName(),
				mongodConfig1.getPort()), mo);
		mongoAdminDB = mongo.getDB(ADMIN_DATABASE_NAME);

		cr = mongoAdminDB.command(new BasicDBObject("isMaster", 1));
		logger.debug("isMaster: " + cr);

		// Initialize replica set
		cr = mongoAdminDB.command(new BasicDBObject("replSetInitiate",
				(DBObject) JSON.parse("{'_id': '" + REPLICA_SET_NAME
						+ "', 'members': [{'_id': 0, 'host': '" + server1
						+ "'}, {'_id': 1, 'host': '" + server2
						+ "'}, {'_id': 2, 'host': '" + server3 + "', 'arbiterOnly' : true}]} }")));
		logger.debug("replSetInitiate: " + cr);

		Thread.sleep(5000);
		cr = mongoAdminDB.command(new BasicDBObject("replSetGetStatus", 1));
		logger.info("replSetGetStatus: " + cr);

		// Check replica set status before from proceed
		while (!isReplicaSetStarted(cr)) {
			logger.debug("Waiting for 3 seconds...");
			Thread.sleep(1000);
			cr = mongoAdminDB.command(new BasicDBObject("replSetGetStatus", 1));
			logger.debug("replSetGetStatus: " + cr);
		}

		mongo.close();
		mongo = null;

		// Initialize a new client using all instances.
		List<ServerAddress> mongoServers = new ArrayList<ServerAddress>();
		mongoServers.add(new ServerAddress(
				Network.getLocalHost().getHostName(), mongodConfig1.getPort()));
		mongoServers.add(new ServerAddress(
				Network.getLocalHost().getHostName(), mongodConfig2.getPort()));
		mongoServers.add(new ServerAddress(
				Network.getLocalHost().getHostName(), mongodConfig3.getPort()));
		mongo = new Mongo(mongoServers, mo);
		Assert.assertNotNull(mongo);
		mongo.setReadPreference(ReadPreference.secondaryPreferred());
	}

	private boolean isReplicaSetStarted(BasicDBObject setting) {
		if (setting.get("members") == null) {
			return false;
		}

		BasicDBList members = (BasicDBList) setting.get("members");
		for (Object m : members.toArray()) {
			BasicDBObject member = (BasicDBObject) m;
			logger.info(member.toString());
			int state = member.getInt("state");
			logger.info("state: " + state);
			// 1 - PRIMARY, 2 - SECONDARY, 7 - ARBITER
			if (state != 1 && state != 2 && state != 7) {
				return false;
			}
		}
		return true;
	}

	private void setupElasticsearchServer() throws Exception {
		logger.debug("*** setupElasticsearchServer ***");
		try {
		Settings settings = 						settingsBuilder()
				.put("path.data", "target/data")
				.put("path.plugins", "target/plugins")
				.put("path.logs", "target/log")
				.put("cluster.name",
						"test-cluster-"
								+ NetworkUtils
										.getLocalAddress())
				.put("gateway.type", "none").build();
        Tuple<Settings, Environment> initialSettings = InternalSettingsPerparer.prepareSettings(settings, true);
        PluginManager pluginManager = new PluginManager(initialSettings.v2(), null);

        if (!initialSettings.v2().pluginsFile().exists()) {
            FileSystemUtils.mkdirs(initialSettings.v2().pluginsFile());
            pluginManager.downloadAndExtract(MAPPER_ATTACHMENT_PLUGIN_NAME);
        } else {
        	logger.info("Plugin {} has been already installed.", MAPPER_ATTACHMENT_PLUGIN_NAME);
        }

        node = nodeBuilder()
				.local(true)
				.settings(settings).node();
		}
		catch (Exception ex) {
			logger.error("setupElasticsearchServer failed", ex);
			throw ex;
		}
	}

	protected void createRiver(String name, String jsonDefinition) throws Exception {
		logger.debug("Create river");
		String mapping = copyToStringFromClasspath("/test/elasticsearch/plugin/river/mongodb/" + jsonDefinition);
		mapping = String.format(mapping, String.valueOf(getMongoPort1()), String.valueOf(getMongoPort2()), String.valueOf(getMongoPort3()));
		logger.debug("River mapping: {}", mapping);
		node.client().prepareIndex("_river", name, "_meta")
				.setSource(mapping).execute().actionGet();
		logger.debug("Running Cluster Health");
        ClusterHealthResponse clusterHealth = node.client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.status());
	}

	protected void deleteRiver(String name) {
    	logger.debug("Delete index [{}]", name);
        node.client().admin().indices().delete(deleteIndexRequest(name)).actionGet();
	}
	
	@AfterSuite
	public void afterSuite() {
		logger.debug("*** afterSuite ***");
		shutdownElasticsearchServer();
		shutdownMongoInstances();
	}

	private void shutdownMongoInstances() {
		logger.debug("*** shutdownMongoInstances ***");
		mongo.close();
		try {
			logger.debug("Start shutdown {}", mongod1);
			mongod1.stop();
		} catch (Throwable t) {
		}
		try {
			logger.debug("Start shutdown {}", mongod2);
			mongod2.stop();
		} catch (Throwable t) {
		}
		try {
			logger.debug("Start shutdown {}", mongod3);
			mongod3.stop();
		} catch (Throwable t) {
		}
	}

	private void shutdownElasticsearchServer() {
		logger.debug("*** shutdownElasticsearchServer ***");
		node.close();
	}

	protected static Mongo getMongo() {
		return mongo;
	}

	protected static Node getNode() {
		return node;
	}

	protected static int getMongoPort1() {
		return mongoPort1;
	}

	protected static int getMongoPort2() {
		return mongoPort2;
	}

	protected static int getMongoPort3() {
		return mongoPort3;
	}
}
