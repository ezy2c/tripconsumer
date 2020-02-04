package au.com.ezy2c.tripconsumer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("application")
public class ApplicationConfig {
    private String kafkaBootstrapServers;
    private String clientId;
    private String kafkaTopic;
    private int maxPollRecords;
    private String offsetReset;
    private long waitTimeSecs;
    private String mysqlURLStart;
    private String dbHost; // hostname for local db 
    private String dbPort; 	// port on which local db is listening for connections
    private String dbUsername; // Username to use to connect to local db
    private String dbPassword; // Password to use to connect to local db
    private String dbSchemaName;
    private String dbConnectionAttributes;
    private int sleepTimeWhenDBConnectExceptionSecs;
    private int sleepTimeWhenSQLExceptionSecs;
    

	public String getKafkaBootstrapServers() {
		return kafkaBootstrapServers;
	}

	public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
		this.kafkaBootstrapServers = kafkaBootstrapServers;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public void setKafkaTopic(String kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
	}

	public int getMaxPollRecords() {
		return maxPollRecords;
	}

	public void setMaxPollRecords(int maxPollRecords) {
		this.maxPollRecords = maxPollRecords;
	}

	public String getOffsetReset() {
		return offsetReset;
	}

	public void setOffsetReset(String offsetReset) {
		this.offsetReset = offsetReset;
	}

	public long getWaitTimeSecs() {
		return waitTimeSecs;
	}

	public void setWaitTimeSecs(long waitTimeSecs) {
		this.waitTimeSecs = waitTimeSecs;
	}

	public String getMysqlURLStart() {
		return mysqlURLStart;
	}

	public void setMysqlURLStart(String mysqlURLStart) {
		this.mysqlURLStart = mysqlURLStart;
	}

	public String getDbSchemaName() {
		return dbSchemaName;
	}

	public void setDbSchemaName(String dbSchemaName) {
		this.dbSchemaName = dbSchemaName;
	}

	public String getDbConnectionAttributes() {
		return dbConnectionAttributes;
	}

	public void setDbConnectionAttributes(String dbConnectionAttributes) {
		this.dbConnectionAttributes = dbConnectionAttributes;
	}

	public int getSleepTimeWhenDBConnectExceptionSecs() {
		return sleepTimeWhenDBConnectExceptionSecs;
	}

	public void setSleepTimeWhenDBConnectExceptionSecs(int sleepTimeWhenDBConnectExceptionSecs) {
		this.sleepTimeWhenDBConnectExceptionSecs = sleepTimeWhenDBConnectExceptionSecs;
	}

	public int getSleepTimeWhenSQLExceptionSecs() {
		return sleepTimeWhenSQLExceptionSecs;
	}

	public void setSleepTimeWhenSQLExceptionSecs(int sleepTimeWhenSQLExceptionSecs) {
		this.sleepTimeWhenSQLExceptionSecs = sleepTimeWhenSQLExceptionSecs;
	}

	public String getDbHost() {
		return dbHost;
	}

	public void setDbHost(String dbHost) {
		this.dbHost = dbHost;
	}

	public String getDbPort() {
		return dbPort;
	}

	public void setDbPort(String dbPort) {
		this.dbPort = dbPort;
	}

	public String getDbUsername() {
		return dbUsername;
	}

	public void setDbUsername(String dbUsername) {
		this.dbUsername = dbUsername;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}


}
