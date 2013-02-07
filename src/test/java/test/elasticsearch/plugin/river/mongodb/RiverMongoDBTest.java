package test.elasticsearch.plugin.river.mongodb;

import static org.elasticsearch.client.Requests.countRequest;
import static org.elasticsearch.client.Requests.getRequest;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertTrue;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.hamcrest.Matcher;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Test
public class RiverMongoDBTest extends RiverMongoDBTestAsbtract {

	private final ESLogger logger = Loggers.getLogger(getClass());

	private static final String DATABASE_NAME = "testriver";
	private static final String COLLECTION_NAME = "person";
	private static final String RIVER_NAME = "testmongodb";
	private static final String INDEX_NAME = "personindex";

	private DB mongoDB;
	private DBCollection mongoCollection;

	@BeforeClass
	public void createDatabase() {
		logger.debug("createDatabase {}", DATABASE_NAME);
		try {
			mongoDB = getMongo().getDB(DATABASE_NAME);
			logger.debug("Create river {}", RIVER_NAME);
			super.createRiver(RIVER_NAME, "test-simple-mongodb-river.json");
			logger.info("Start createCollection");
			mongoCollection = mongoDB.createCollection(COLLECTION_NAME, null);
			Assert.assertNotNull(mongoCollection);
		} catch (Throwable t) {
			logger.error("createDatabase failed.", t);
		}
	}

	@AfterClass
	public void cleanUp() {
		super.deleteRiver(INDEX_NAME);
		logger.info("Drop database " + mongoDB.getName());
		mongoDB.dropDatabase();
	}

	@Test
	public void simpleBSONObject() throws Throwable {
		logger.debug("Start simpleBSONObject");
		try {
			String mongoDocument = copyToStringFromClasspath("/test/elasticsearch/plugin/river/mongodb/test-simple-mongodb-document.json");
			DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
			WriteResult result = mongoCollection.insert(dbObject,
					WriteConcern.REPLICAS_SAFE);
			Thread.sleep(1000);
			String id = dbObject.get("_id").toString();
			logger.info("WriteResult: {}", result.toString());
			getNode().client().admin().indices()
					.refresh(new RefreshRequest(INDEX_NAME));
			ActionFuture<IndicesExistsResponse> response = getNode().client()
					.admin().indices()
					.exists(new IndicesExistsRequest(INDEX_NAME));
			assertThat(response.actionGet().isExists(), equalTo(true));
			Thread.sleep(5000);
			SearchResponse searchResponse = getNode()
          .client().prepareSearch(INDEX_NAME).setQuery(fieldQuery("name", "Richard")).execute().actionGet();
			logger.info("Document count: {}", searchResponse.hits().totalHits());

      SearchHit hit = searchResponse.getHits().getAt(0);

      Map<String,Object> sourceDoc = hit.getSource();
      assertThat(searchResponse.hits().totalHits(), equalTo(1l));

      List<Object> searchValues = (List<Object>) sourceDoc.get("search");
      List<Object> search2Values = (List<Object>) sourceDoc.get("search2");
      assertEquals(searchValues.size(),2);
      assertEquals(search2Values.size(),2);
      assertTrue(searchValues.containsAll(Arrays.asList("English","My ticker symbol")));
      assertTrue(search2Values.containsAll(Arrays.asList("English","My ticker symbol")));
			//assertTrue(sourceDoc.get("op") == null);
			//assertTrue(sourceDoc.get("ts") == null);

      mongoCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);

			Thread.sleep(5000);
			getNode().client().admin().indices()
					.refresh(new RefreshRequest(INDEX_NAME));
      CountResponse countResponse = getNode()
					.client()
					.count(countRequest(INDEX_NAME)
							.query(fieldQuery("_id", id))).actionGet();
			logger.debug("Count after delete request: {}", countResponse.count());
			 assertThat(countResponse.count(), equalTo(0L));
		} catch (Throwable t) {
			logger.error("importAttachment failed.", t);
			t.printStackTrace();
			throw t;
		}
	}

}
