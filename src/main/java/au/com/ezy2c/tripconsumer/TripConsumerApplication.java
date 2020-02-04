package au.com.ezy2c.tripconsumer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import au.com.ezy2c.common.DBConnectException;
import au.com.ezy2c.common.DBUtil;
import au.com.ezy2c.common.DateUtil;

@SpringBootApplication
public class TripConsumerApplication implements CommandLineRunner {
	private static Logger LOGGER;    

	@Autowired
	private ApplicationConfig applicationConfig;
	
	@Autowired 
	Consumer<String, String> consumer;
	
	@Autowired
	GPSLogIndex gpslogIndex;
	
	@Bean
	public Consumer<String, String> getConsumer() {
	    Properties props = new Properties();
	    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,applicationConfig.getKafkaBootstrapServers());
	    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
	    props.put(ConsumerConfig.CLIENT_ID_CONFIG,UUID.randomUUID().toString()); // applicationConfig.getClientId());
	    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, applicationConfig.getMaxPollRecords());
	    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
	    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, applicationConfig.getOffsetReset());
	    Consumer<String, String> consumer = new KafkaConsumer<>(props);
	    consumer.subscribe(Collections.singletonList(applicationConfig.getKafkaTopic()));
	    return consumer;
	
	}
	/**
	 * @param trigger
	 * @param dbSchemaName
	 * @param gpslogTableName
	 */
	private void insertTriggerList(final long threadId, final String url, final String dbSchemaName, final String fullGpslogTableName, List<Trigger> triggerList) throws DBConnectException, SQLException {
		String sql = "INSERT INTO "+dbSchemaName+"." + fullGpslogTableName + " (" + Trigger.insertColumns(false,false) + ") "
				+" VALUES (" + Trigger.insertParams(false) + ")  "
				+ " ON DUPLICATE KEY UPDATE analogue1 = ? ";
		LOGGER.log(Level.FINE, "Thread id {0}: Inserting triggers using sql: {1} ",new Object[] {threadId, sql});
		Connection c = null;
		PreparedStatement ps = null;
		try {
			c = DBUtil.mySQLConnect(url);
			ps = c.prepareStatement(sql);
			if (triggerList != null) {
				int batchSize = 0;
				for (Trigger trigger : triggerList) {
					int i = trigger.substituteParams(ps, 1, false);
					ps.setFloat(i++, trigger.getAnalogue1());
					ps.addBatch();
					if (trigger.getUnitIdInt()==48270) {
						LOGGER.log(Level.INFO,"Inserting 48270 record using sql {0} and utc_time {1} trigger {2} : full trigger {3}",new Object[]{sql, trigger.getUtcTime(), trigger.getTrigger(), trigger});
					}
					batchSize++;
				}
				LOGGER.log(Level.INFO,"Thread id {0}: Inserting {1} triggers into {2} ",new Object[] {threadId,batchSize,fullGpslogTableName});
				ps.executeUpdate();
			}			
		} catch (DBConnectException ex) {
			String msg = "Thread id "+threadId+": ERROR: DBConnectException: Trying to connect to the database "+dbSchemaName+"."+fullGpslogTableName+" using sql "+sql+" : "+ ex.getMessage();
			LOGGER.log(Level.SEVERE,msg);
			throw ex;
		} catch (SQLException ex) {
			int ex_val = ex.getErrorCode();
			if (ex_val == 1062) {
				LOGGER.log(Level.WARNING,"Thread id {0}: WARNING: 1062 Error writing to {1}.{2} : Ignoring record",new Object[] {threadId,dbSchemaName,fullGpslogTableName});
			} else {
				String msg = "Thread id "+threadId+": ERROR: SQLException: writing to "+dbSchemaName+"."+fullGpslogTableName+" using sql "+sql+" : "+ ex.getMessage();
				LOGGER.log(Level.SEVERE,msg);
				throw ex;
			}
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Throwable th) {
				}
			}
			if (c != null) {
				try {
					c.close();
				} catch (Throwable th) {
				}
			}
		}
		LOGGER.log(Level.INFO, "ThreadId {0}: Insert complete",threadId);
	}
	/**
	 * @param trigger
	 * @param dbSchemaName
	 * @param gpslogTableName
	 */
	private void saveOffset(String url, String dbSchemaName, long offset) throws DBConnectException, SQLException {
		String sql = "UPDATE "+dbSchemaName+".consumerOffset SET `offset` = ? ";
		LOGGER.log(Level.INFO, "Saving offset using sql: {1} ",new Object[] {sql});
		Connection c = null;
		PreparedStatement ps = null;
		try {
			c = DBUtil.mySQLConnect(url);
			ps = c.prepareStatement(sql);
			int i = 1;
			ps.setLong(i++, offset);
			ps.executeUpdate();
		} catch (DBConnectException ex) {
			String msg = "ERROR: DBConnectException: Trying to connect to the database "+url+" : "+ ex.getMessage();
			LOGGER.log(Level.SEVERE,msg);
			throw ex;
		} catch (SQLException ex) {
			int ex_val = ex.getErrorCode();
			if (ex_val == 1062) {
				LOGGER.log(Level.WARNING,"WARNING: 1062 Error writing to {1}.consumerOffset : Ignoring ",new Object[] {dbSchemaName});
			} else {
				String msg = "ERROR: SQLException: writing to "+dbSchemaName+".consumerOffset using sql "+sql+" : "+ ex.getMessage();
				LOGGER.log(Level.SEVERE,msg);
				throw ex;
			}
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Throwable th) {
				}
			}
			if (c != null) {
				try {
					c.close();
				} catch (Throwable th) {
				}
			}
		}
		LOGGER.log(Level.INFO, "Insert complete");
	}
	/**
	 * @param trigger
	 * @param dbSchemaName
	 * @param gpslogTableName
	 */
	private long loadOffset(String url, String dbSchemaName) throws DBConnectException, SQLException {
		String sql = "SELECT offset FROM "+dbSchemaName+".consumerOffset ";
		LOGGER.log(Level.INFO, "Selecting offset using sql: {0} ",new Object[] {sql});
		Connection c = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			c = DBUtil.mySQLConnect(url);
			ps = c.prepareStatement(sql);
			rs = ps.executeQuery();
			long offset = 0l;
			if (rs.next()) {
				offset = rs.getLong(1);
			} else {
				LOGGER.log(Level.INFO,"Need to insert the first row into the consumerOffset table");
				rs.close();
				ps.close();
				ps = c.prepareStatement("INSERT INTO "+dbSchemaName+".consumerOffset(`offset`) values (0)");
				ps.executeUpdate();
			}
			LOGGER.log(Level.INFO, "Returning offset {0} ",new Object[] {offset});
			return offset;
		} catch (DBConnectException ex) {
			String msg = "ERROR: DBConnectException: Trying to connect to the database "+url+" : "+ ex.getMessage();
			LOGGER.log(Level.SEVERE,msg);
			throw ex;
		} catch (SQLException ex) {
			String msg = "ERROR(: SQLException: selecting from "+dbSchemaName+".consumerOffset using sql "+sql+" : "+ ex.getMessage();
			LOGGER.log(Level.SEVERE,msg);
			throw ex;
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} 
				catch (Throwable th) {
				
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Throwable th) {
				}
			}
			if (c != null) {
				try {
					c.close();
				} catch (Throwable th) {
				}
			}
		}
	}
	@Override
	public void run(String... args) throws Exception {
		Duration waitTime = Duration.ofSeconds(applicationConfig.getWaitTimeSecs()); 
		String dbSchemaName = applicationConfig.getDbSchemaName();
    	String fullDbName = applicationConfig.getDbHost()+":"+applicationConfig.getDbPort()+"/"+dbSchemaName;
    	String dbURLParams = "?user="+applicationConfig.getDbUsername()+"&password="+applicationConfig.getDbPassword()+"&"+applicationConfig.getDbConnectionAttributes();
		final String url = applicationConfig.getMysqlURLStart()+fullDbName+dbURLParams;
	    long offset = loadOffset(url, dbSchemaName);
	    boolean needToSeek = offset > 0l;
        while (true) {	
        	ConsumerRecords<String, String> consumerRecords = consumer.poll(waitTime);
        	// At this point, assignment should have occurred and so if we need to seek, do so and then poll again from the new position
        	if (needToSeek) {
        		needToSeek = false;
        	    Set<TopicPartition> partitions = consumer.assignment();
            	LOGGER.log(Level.INFO,"Need to seek. There are {0} partitions",new Object[] {partitions.size()});
        	    for (TopicPartition topicPartition : partitions) {
        	    	LOGGER.log(Level.INFO,"Setting partition {0} to offset {1}",new Object[] {topicPartition,offset});
        	    	consumer.seek(topicPartition, offset);
        		}
        	    continue;	// Poll again
        	}
        	int count = consumerRecords.count();
        	long latestUtcDateTime = 0l;
        	if (count > 0) {
        		
        		// Connect to the database
        		Connection c = null;
				//Trigger trigger = null;
				Map<String,List<Trigger>> triggersMap = new HashMap<>();
        		try {
        			c = DBUtil.mySQLConnect(url);
        			ConsumerRecord<String, String> record = null;
	        		for (Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator(); iterator.hasNext(); ) {
	        			record = iterator.next();
	        			LOGGER.log(Level.FINE,"Record: Partition {0} Offset {1} Key {2} Value {3}",new Object[] {record.partition(),record.offset(),record.key(),record.value()});
	        			//LOGGER.log(Level.INFO,"Record: Partition {0} Offset {1}",new Object[] {record.partition(), record.offset()});
	        		
	        			// Determine which gpslog table to put the record in
	        			Integer unitId = null;
	        			String serverId = null;
	        			String gpslogTableName = null;
	        			try {
		    				JSONObject keyJson = new JSONObject(record.key());
		    				TriggerKey key = new TriggerKey(keyJson);
		    				serverId = key.getServerId();
		    				unitId = key.getUnitId();
		    				gpslogTableName = gpslogIndex.findGpslogTableName(c,unitId);
	        			} catch (SQLException ex) {
	        				LOGGER.log(Level.SEVERE,"Unable to find the gpslog table name for the unitId {0}: SQLException caught: {1} : Ignoring record",new Object[] {unitId,ex.getMessage()});
	        			} catch (JSONException ex) {
	        				LOGGER.log(Level.SEVERE,"Unable to convert the key to JSON or to a TriggerKey object. The key is {0} : JSONException: {1} : Ignoring record",new Object[] {record.key(),ex.getMessage()});
	           			}
	        			
	    				// Store the record
	        			if (unitId != null && serverId != null && gpslogTableName != null) {
	        				Trigger trigger = null;
		        			try {
			    				JSONObject valueJson = new JSONObject(record.value());
			 	        		trigger = new Trigger(valueJson);	
			 	        		long triggerUtcDateTime = trigger.getUtcTime().getTime();
			 	        		if (triggerUtcDateTime > latestUtcDateTime)
			 	        			latestUtcDateTime = triggerUtcDateTime;
		        			} catch (ParseException ex) {
		        				LOGGER.log(Level.SEVERE,"Unable to convert the value to a Trigger object. The value is {0} : ParseException: {1} : Ignoring record",new Object[] {record.value(),ex.getMessage()});
		        			} catch (JSONException ex) {
		        				LOGGER.log(Level.SEVERE,"Unable to convert the value to JSON or to a Trigger object. The value is {0} : JSONException: {1} : Ignoring record",new Object[] {record.value(),ex.getMessage()});
		           			}
		        			String fullGpslogTableName = gpslogTableName+serverId;
		        			LOGGER.setLevel(Level.ALL);
		        			if (trigger != null) {
		        				List<Trigger> triggersList = triggersMap.get(fullGpslogTableName);
		        				if (triggersList == null) {
		        					triggersList = new ArrayList<Trigger>();
		        					triggersMap.put(fullGpslogTableName,triggersList);
		        				}
		        				triggersList.add(trigger);
		        			}
	        			} else {
	        				LOGGER.log(Level.WARNING,"Unable to process trigger with null as one of the unitId, serverId, or gpslogTableName fields. unitId is {0} serverId is {1} gpslogTableName is {2} and key is {3} : Ignoring record ",new Object[] {unitId, serverId, gpslogTableName, record.key()});
	        			}
	        		} 
	        		// Insert all the triggers in the triggersMap in batches
	        		LOGGER.log(Level.INFO,"Inserting {0} triggers ...",count);
	        		List<Thread> allThreads = new ArrayList<>();
	        		for (String fullGpslogTableName : triggersMap.keySet()) {
	        			List<Trigger> triggerList = triggersMap.get(fullGpslogTableName);
	        			if (triggerList != null) {
		        			Thread thread = new Thread(new Runnable() {
		        				public void run() {
		        					try {
		        						insertTriggerList(Thread.currentThread().getId(),url,dbSchemaName,fullGpslogTableName,triggerList);
		        					} catch (DBConnectException ex) {
		        	        			LOGGER.log(Level.WARNING, "Unable to connect to the database: SQLException {0} - ignoring",ex.getMessage());
		        					} catch (SQLException ex) {
		        	        			LOGGER.log(Level.WARNING, "Unable to insert the triggers: SQLException {0} - ignoring",ex.getMessage());
		        					}
		        				}
		        			});
		        			allThreads.add(thread);
		        			thread.start();
	        			}
	        		}
	        		
	        		// Wait for all threads to complete
	        		int size = allThreads.size();
	        		LOGGER.log(Level.INFO,"Waiting for {0} threads to complete with latest UTC Time {1} => {2} behind ...",new Object[] {size,latestUtcDateTime==0?"0":DateUtil.convertDateAndTimeToString(new Date(latestUtcDateTime)),DateUtil.durationFromNow(latestUtcDateTime)});
	        		int i = 0;
	        		for (Thread thread : allThreads) {
		        		LOGGER.log(Level.INFO,"Waiting for thread num {0} of {1} with id {2} ...", new Object[] {i++,size,thread.getId()});
	        			thread.join();
	        		}
	        	
	        		// Commits the offset of record to broker
	        		LOGGER.log(Level.INFO,"Commiting {0} records ...",count);
	        		consumer.commitAsync();	          
	        		LOGGER.log(Level.INFO,"... commit of {0} records complete",count);
	        		// Save the offset to the database
	        		saveOffset(url,dbSchemaName,record.offset());
          		} catch (DBConnectException ex) {
        			LOGGER.log(Level.SEVERE, "Unable to connect to the Trip Processor database using URL {0}: DBConnectException {1}",new Object[] {url, ex});
        			try {
        				Thread.sleep(applicationConfig.getSleepTimeWhenDBConnectExceptionSecs()*1000);
        			} catch (InterruptedException e) {
        				// Ignore
        			}
        		} finally {
        			// Close the database connection 
            		if (c != null) {
            			try {
            				c.close();
            				c = null;
            			} catch (Throwable th) {
            				LOGGER.log(Level.WARNING,"Unable to close the database connection: Throwable caught: {0}",th.getMessage());
            			}
            		}
        		}
        	}	
        }
        
        //LOGGER.log(Level.INFO,"Main finished");
	}
	public static Set<Level> getAllLevels() throws IllegalAccessException {
		Class<Level> levelClass = Level.class;

		Set<Level> allLevels = new TreeSet<>(Comparator.comparingInt(Level::intValue));

		for (Field field : levelClass.getDeclaredFields()) {
			if (field.getType() == Level.class) {
				allLevels.add((Level) field.get(null));
			}
		}
		return allLevels;
	}   

	public static void main(String[] args) throws Exception {
		LogManager.getLogManager().reset();    //reset() will remove all default handlers
		LOGGER = LogManager.getLogManager().getLogger("");
		ConsoleHandler handler = new ConsoleHandler();
		handler.setFormatter(new SimpleFormatter());
		handler.setLevel(Level.ALL);
		LOGGER.addHandler(handler);
		LOGGER.log(Level.INFO,"Main started");
    	try {
    		LogManager.getLogManager().readConfiguration(TripConsumerApplication.class.getClassLoader().getResourceAsStream("logging.properties"));
    	} catch (IOException ex) {
    	    System.err.println("WARNING: Could not open configuration file");
    	    System.err.println("WARNING: Logging not configured (console output only)");
    	}
    	LOGGER.setUseParentHandlers(false);
    	Set<Level> levels = getAllLevels();
        int i = 1;
        for (Level level : levels) {
            LOGGER.log(level, level.getName() + " - " + (i++));
        }
        SpringApplication.run(TripConsumerApplication.class, args);
	}

}
