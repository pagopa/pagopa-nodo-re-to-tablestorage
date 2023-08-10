package it.gov.pagopa.nodoretodatastore;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.OutputBinding;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.Cardinality;
import com.microsoft.azure.functions.annotation.CosmosDBOutput;
import com.microsoft.azure.functions.annotation.EventHubTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import it.gov.pagopa.nodoretodatastore.util.ObjectMapperUtils;
import lombok.NonNull;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * Azure Functions with Azure Event Hub trigger.
 */
public class NodoReEventToDataStore {
    /**
     * This function will be invoked when an Event Hub trigger occurs
     */

	private Pattern replaceDashPattern = Pattern.compile("-([a-zA-Z])");
	private static String NA = "NA";
	private static String idField = "uniqueId";
//	private static String tableName = System.getenv("TABLE_STORAGE_TABLE_NAME");
	private static String insertedTimestamp = "insertedTimestamp";
	private static String insertedDate = "insertedDate";
	private static String partitionKey = "PartitionKey";
	private static String payloadField = "payload";

//	private static MongoClient mongoClient = null;

//	private static TableServiceClient tableServiceClient = null;

//	private static MongoClient getMongoClient(){
//		if(mongoClient==null){
//			mongoClient = new MongoClient(new MongoClientURI(System.getenv("COSMOS_CONN_STRING")));
//		}
//		return mongoClient;
//	}

//	private static TableServiceClient getTableServiceClient(){
//		if(tableServiceClient==null){
//			tableServiceClient = new TableServiceClientBuilder().connectionString(System.getenv("TABLE_STORAGE_CONN_STRING"))
//					.buildClient();
//			tableServiceClient.createTableIfNotExists(tableName);
//		}
//		return tableServiceClient;
//	}


//	private void addToBatch(Logger logger, Map<String,List<TableTransactionAction>> partitionEvents, Map<String,Object> reEvent){
//		if(reEvent.get(idField) == null) {
//			logger.warning("event has no '" + idField + "' field");
//		} else {
//			TableEntity entity = new TableEntity((String) reEvent.get(partitionKey), (String)reEvent.get(idField));
//			entity.setProperties(reEvent);
//			if(!partitionEvents.containsKey(entity.getPartitionKey())){
//				partitionEvents.put(entity.getPartitionKey(),new ArrayList<TableTransactionAction>());
//			}
//			partitionEvents.get(entity.getPartitionKey()).add(new TableTransactionAction(TableTransactionActionType.UPSERT_REPLACE,entity));
//		}
//	}

	private String replaceDashWithUppercase(String input) {
		if(!input.contains("-")){
			return input;
		}
		Matcher matcher = replaceDashPattern.matcher(input);
		StringBuffer sb = new StringBuffer();

		while (matcher.find()) {
			matcher.appendReplacement(sb, matcher.group(1).toUpperCase());
		}
		matcher.appendTail(sb);

		return sb.toString();
	}

	private void zipPayload(Logger logger,Map<String,Object> reEvent) {
		if(reEvent.get(payloadField)!=null){
			try {
				byte[] data = ((String)reEvent.get(payloadField)).getBytes(StandardCharsets.UTF_8);
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				Deflater deflater = new Deflater();
				DeflaterOutputStream dos = new DeflaterOutputStream(baos, deflater);
				dos.write(data);
				dos.close();
				reEvent.put(payloadField,baos.toByteArray());
			} catch (Exception e) {
				logger.severe(e.getMessage());
			}
		}
	}

    @FunctionName("EventHubNodoReEventProcessor")
    public void processNodoReEvent (
            @EventHubTrigger(
                    name = "NodoReEvent",
                    eventHubName = "", // blank because the value is included in the connection string
                    connection = "EVENTHUB_CONN_STRING",
                    cardinality = Cardinality.MANY)
    		List<String> reEvents,
    		@BindingName(value = "PropertiesArray") Map<String, Object>[] properties,
			@CosmosDBOutput(
					name = "NodoReEventToDataStore",
					databaseName = "nodo_re",
					containerName = "events",
					createIfNotExists = false,
					connection = "COSMOS_CONN_STRING")
			@NonNull OutputBinding<List<Object>> documentdb,
            final ExecutionContext context) {

		Logger logger = context.getLogger();

//		TableClient tableClient = getTableServiceClient().getTableClient(tableName);
		String msg = String.format("Persisting %d events", reEvents.size());
		logger.info(msg);
        try {
        	if (reEvents.size() == properties.length) {
//				Map<String,List<TableTransactionAction>> partitionEvents = new HashMap<>();
				List<Object> eventsToPersistCosmos = new ArrayList<>();

				for(int index=0; index< properties.length; index++) {
					// logger.info("processing "+(index+1)+" of "+properties.length);
					final Map<String,Object> reEvent = ObjectMapperUtils.readValue(reEvents.get(index), Map.class);
					properties[index].forEach((p,v) -> {
						String s = replaceDashWithUppercase(p);
						reEvent.put(s, v);
					});

					reEvent.put("id", reEvent.get("uniqueId"));

					String insertedDateValue = reEvent.get(insertedTimestamp) != null ? ((String)reEvent.get(insertedTimestamp)).substring(0, 10) : NA;
					reEvent.put(insertedDate, insertedDateValue);

					String idDominio = reEvent.get("idDominio") != null ? reEvent.get("idDominio").toString() : NA;
					String idPsp = reEvent.get("psp") != null ? reEvent.get("psp").toString() : NA;
					String partitionKeyValue = insertedDateValue + "-" + idDominio + "-" + idPsp;
					reEvent.put(partitionKey, partitionKeyValue);

//					zipPayload(logger,reEvent);
					reEvent.put(payloadField, null);

//					addToBatch(logger,partitionEvents,reEvent);
					eventsToPersistCosmos.add(reEvent);
				}

//				partitionEvents.forEach((pe,values)->{
//					try {
//						tableClient.submitTransaction(values);
//					} catch (Throwable t){
//						logger.severe("Could not save on tableStorage,partition "+pe+", "+values.size()+" rows,error:"+ t.toString());
//					}
//				});

				try {
					documentdb.setValue(eventsToPersistCosmos);
				} catch (Throwable t){
					logger.severe("Could not save on cosmos "+eventsToPersistCosmos.size()+", error:"+ t.toString());
				}

				logger.info("Done processing events");
            } else {
				logger.severe("Error processing events, lengths do not match ["+reEvents.size()+","+properties.length+"]");
            }
        } catch (NullPointerException e) {
            logger.severe("NullPointerException exception on cosmos nodo-re-events msg ingestion at "+ LocalDateTime.now()+ " : " + e.getMessage());
        } catch (Throwable e) {
            logger.severe("Generic exception on cosmos nodo-re-events msg ingestion at "+ LocalDateTime.now()+ " : " + e.getMessage());
        }

    }
}
