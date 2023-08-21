package it.gov.pagopa.nodoretotablestorage;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableTransactionAction;
import com.azure.data.tables.models.TableTransactionActionType;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.Cardinality;
import com.microsoft.azure.functions.annotation.EventHubTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import it.gov.pagopa.nodoretotablestorage.util.ObjectMapperUtils;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
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
	private static String na = "NA";
	private static String uniqueIdField = "uniqueId";
	private static String insertedDateField = "insertedDate";
	private static String insertedTimestampField = "insertedTimestamp";
	private static String partitionKeyField = "PartitionKey";
	private static String payloadField = "payload";
	private static String idDominioField = "idDominio";

	private static TableServiceClient tableServiceClient = null;

	private static String tableName = System.getenv("TABLE_STORAGE_TABLE_NAME");

	private static TableServiceClient getTableServiceClient(){
		if(tableServiceClient==null) {
			tableServiceClient = new TableServiceClientBuilder().connectionString(System.getenv("TABLE_STORAGE_CONN_STRING"))
					.buildClient();
			tableServiceClient.createTableIfNotExists(tableName);
		}
		return tableServiceClient;
	}

	private void addToBatch(Logger logger, Map<String,List<TableTransactionAction>> partitionEvents, Map<String, Object> reEvent) {
		if(reEvent.get(uniqueIdField) != null) {
			TableEntity entity = new TableEntity((String) reEvent.get(partitionKeyField), (String)reEvent.get(uniqueIdField));
			entity.setProperties(reEvent);
			if(!partitionEvents.containsKey(entity.getPartitionKey())){
				partitionEvents.put(entity.getPartitionKey(),new ArrayList<>());
			}
			partitionEvents.get(entity.getPartitionKey()).add(new TableTransactionAction(TableTransactionActionType.UPSERT_REPLACE,entity));
		}
		else {
			logger.warning(String.format("event has no '%s' field", uniqueIdField));
		}
	}

	private String replaceDashWithUppercase(String input) {
		if(!input.contains("-")) {
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

	private void zipPayload(Logger logger, Map<String,Object> reEvent) {
		if(reEvent.get(payloadField)!=null) {
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

	private Map<String,Object> getEvent(String partitionKeyValue, Map<String,Object> reEvent) {
		reEvent.put(partitionKeyField, partitionKeyValue);
		return reEvent;
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
            final ExecutionContext context) {

		Logger logger = context.getLogger();

		TableClient tableClient = getTableServiceClient().getTableClient(tableName);

		try {
			if (reEvents.size() == properties.length) {
				logger.info(String.format("Persisting %d events", reEvents.size()));

				Map<String,List<TableTransactionAction>> partitionEvents = new HashMap<>();

				for(int index=0; index< properties.length; index++) {
					final Map<String,Object> reEvent = ObjectMapperUtils.readValue(reEvents.get(index), Map.class);
					properties[index].forEach((p,v) -> {
						String s = replaceDashWithUppercase(p);
						reEvent.put(s, v);
					});

					String insertedDateValue = reEvent.get(insertedTimestampField) != null ? ((String)reEvent.get(insertedTimestampField)).substring(0, 10) : na;
					reEvent.put(insertedDateField, insertedDateValue);

					zipPayload(logger, reEvent);

					String idDominio = reEvent.get(idDominioField) != null ? reEvent.get(idDominioField).toString() : na;

					addToBatch(logger, partitionEvents, getEvent(insertedDateValue, reEvent));
					addToBatch(logger, partitionEvents, getEvent(insertedDateValue + "-" + idDominio, reEvent));
				}

				// save batch by partition keys
				partitionEvents.forEach((pe, values) -> {
					try {
						tableClient.submitTransaction(values);
					} catch (Exception t) {
						logger.severe("Could not save on tableStorage,partition "+pe+", "+values.size()+" rows,error:"+ t.toString());
					}
				});

				logger.info("Done processing events");
            } else {
				logger.severe("Error processing events, lengths do not match ["+reEvents.size()+","+properties.length+"]");
            }
        } catch (NullPointerException e) {
            logger.severe("NullPointerException exception on cosmos nodo-re-events msg ingestion at "+ LocalDateTime.now()+ " : " + e.getMessage());
        } catch (Exception e) {
            logger.severe("Generic exception on table storage nodo-re-events msg ingestion at "+ LocalDateTime.now()+ " : " + e.getMessage());
        }
    }
}
