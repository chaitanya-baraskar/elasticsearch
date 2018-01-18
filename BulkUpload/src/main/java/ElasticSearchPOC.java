
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.vividsolutions.jts.io.ParseException;


public class ElasticSearchPOC {

	
	// Tried Transport client
	private RestHighLevelClient client;
	TransportClient transportClient;

	public ElasticSearchPOC() throws UnknownHostException {
		client = new RestHighLevelClient(RestClient.builder(new HttpHost("10.43.14.98", 9200, "http")));
		Settings settings = Settings.builder().put("cluster.name","testcluster").build();
		transportClient = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("10.43.14.98"),9300));
	}

	public void deleteIndex() throws IOException {

		DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("tweet");
		deleteIndexRequest.timeout(TimeValue.timeValueMillis(5000));
		deleteIndexRequest.masterNodeTimeout(TimeValue.timeValueMillis(5000));

		try {
			DeleteIndexResponse deleteIndexResponse = client.indices().deleteIndex(deleteIndexRequest);
			boolean ack = deleteIndexResponse.isAcknowledged();
			System.out.println("Response from Client ----->  " + ack);
		} catch (ElasticsearchException e) {
			if(e.status() == RestStatus.NOT_FOUND) {
				System.out.println("Counldn't find Index");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createIndex() throws IOException {

		String jsonString = "{" +
				"\"user\":\"kimchy\"," +
				"\"postDate\":\"2013-01-30\"," +
				"\"message\":\"trying out Elasticsearch\"" +
				"}";

		IndexRequest indexRequest = new IndexRequest("test","doc","1");

		indexRequest.source(jsonString,XContentType.JSON);

		try {
			IndexResponse indexResponse = client.index(indexRequest);
			System.out.println("Request STatus " + indexResponse.status());
		} catch (ElasticsearchException e) {
			if(e.status() == RestStatus.NOT_FOUND) {
				System.out.println("Counldn't create Index");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void clientClose() throws IOException {
		client.close();
	}

	public void getIndexData() {
		GetRequest getRequest = new GetRequest("test","doc","1");
		try {
			GetResponse getResponse = client.get(getRequest);
			Map<String, Object> sourceAsString = getResponse.getSourceAsMap();
			System.out.println(sourceAsString.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void deleteData() {
		DeleteRequest deleteRequest = new DeleteRequest("test","doc","1");
		try {
			DeleteResponse deleteResponse = client.delete(deleteRequest);
			System.out.println(deleteResponse.status());
		} catch (ElasticsearchException e) {
			if(e.status() == RestStatus.CONFLICT) {
				System.out.println("Couldn't find record to delete");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void updateData() {
		UpdateRequest request = new UpdateRequest("test","doc","1");
		String jsonString = "{" +
				"\"updated\":\"2017-01-01\"," +
				"\"reason\":\"daily update\"" +
				"}";
		request.doc(jsonString, XContentType.JSON);

		//request.doc("message","Updated Message");

		try {
			UpdateResponse response = client.update(request);
			System.out.println(response.status());
			if (response.getResult() == DocWriteResponse.Result.CREATED) {
				System.out.println("Document Created");
			} else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
				System.out.println("Document Updated");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void bulkAPI() {
		BulkRequest bulkRequest = new BulkRequest();
		bulkRequest.add(new IndexRequest("test","doc","1").source(XContentType.JSON, "field", "foo"));
		bulkRequest.add(new IndexRequest("test","doc","2").source(XContentType.JSON, "field", "bar"));
		bulkRequest.add(new IndexRequest("test","doc","3").source(XContentType.JSON, "field", "GG"));

		try {
			BulkResponse bulkResponse = client.bulk(bulkRequest);
			System.out.println(bulkResponse.status());
			if(bulkResponse.hasFailures()) {

			}
		} catch (IOException e) {
			e.printStackTrace();
		}


	}

	//With REST CLient
	public void bulkProcessorRest() {

		BulkProcessor.Listener listener= new BulkProcessor.Listener() {

			public void beforeBulk(long arg0, BulkRequest arg1) {
				System.out.println("Number of Actions --->  " + arg1.numberOfActions());
			}

			public void afterBulk(long arg0, BulkRequest arg1, Throwable arg2) {
				System.out.println("Failed to execute bulk ---> " + arg2);
			}

			public void afterBulk(long arg0, BulkRequest arg1, BulkResponse arg2) {

				if(arg2.hasFailures()) {
					System.out.println("Bulk Processed with failure");
				}else {
					System.out.println( arg0 +" Bulk Completed in - " + arg2.getTook().getMillis() );
				}
				System.out.println("Execution State --->" + arg2.hasFailures());
			}
		};

		/*BulkProcessor bulkProcessor = BulkProcessor.builder(client::bulkAsync, listener).build();*/
		BulkProcessor.Builder builder = BulkProcessor.builder(client::bulkAsync, listener);
		builder.setBulkActions(500);
		builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
		builder.setConcurrentRequests(0);
		builder.setFlushInterval(TimeValue.timeValueMillis(500));
		builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(500), 3));

		IndexRequest one = new IndexRequest("test", "doc", "1").
				source(XContentType.JSON, "title", "In which order are my Elasticsearch queries executed?");
		IndexRequest two = new IndexRequest("test", "doc", "1").
				source(XContentType.JSON, "title", "In which order are my Elasticsearch queries executed?");
		IndexRequest three = new IndexRequest("test", "doc", "1").
				source(XContentType.JSON, "title", "In which order are my Elasticsearch queries executed?");

		BulkProcessor bulkProcessor = builder.build();
		bulkProcessor.add(one);
		bulkProcessor.add(two);
		bulkProcessor.add(three);

		try {
			boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
			System.out.println("Connection Closed ---> " + terminated);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void bulkProcessorJava() {

		BulkProcessor.Listener listener= new BulkProcessor.Listener() {

			public void beforeBulk(long arg0, BulkRequest arg1) {
				System.out.println("Number of Actions --->  " + arg1.numberOfActions());
			}

			public void afterBulk(long arg0, BulkRequest arg1, Throwable arg2) {
				System.out.println("Failed to execute bulk ---> " + arg2);
			}

			public void afterBulk(long arg0, BulkRequest arg1, BulkResponse arg2) {

				if(arg2.hasFailures()) {
					System.out.println("Bulk Processed with failure");
				}else {
					System.out.println( arg0 +" Bulk Completed in - " + arg2.getTook().getMillis() );
				}
				System.out.println("Execution State --->" + arg2.hasFailures());
			}
		};

		/*BulkProcessor bulkProcessor = BulkProcessor.builder(client::bulkAsync, listener).build();*/
		BulkProcessor.Builder builder = BulkProcessor.builder(transportClient, listener);
		builder.setBulkActions(500);
		builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
		builder.setConcurrentRequests(0);
		builder.setFlushInterval(TimeValue.timeValueMillis(500));
		builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(500), 3));

		IndexRequest one = new IndexRequest("test", "doc", "1").
				source(XContentType.JSON, "title", "In which order are my Elasticsearch queries executed?");
		IndexRequest two = new IndexRequest("test", "doc", "1").
				source(XContentType.JSON, "title", "In which order are my Elasticsearch queries executed?");
		IndexRequest three = new IndexRequest("test", "doc", "1").
				source(XContentType.JSON, "title", "In which order are my Elasticsearch queries executed?");

		BulkProcessor bulkProcessor = builder.build();
		bulkProcessor.add(one);
		bulkProcessor.add(two);
		bulkProcessor.add(three);

		try {
			boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
			System.out.println("Connection Closed ---> " + terminated);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws ParseException, IOException {
		ElasticSearchPOC elasticSearchIndex = new ElasticSearchPOC();
		//elasticSearchIndex.deleteIndex();
		//elasticSearchIndex.createIndex();
		elasticSearchIndex.getIndexData();
		//elasticSearchIndex.bulkProcessorRest();
		//elasticSearchIndex.getIndexData();
		//elasticSearchIndex.updateData();
		//elasticSearchIndex.bulkAPI();
		//elasticSearchIndex.deleteData();
		//elasticSearchIndex.bulkProcessorJava();
		elasticSearchIndex.clientClose();
	}
}
