package au.com.ezy2c.tripconsumer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import au.com.ezy2c.common.DateUtil;
import au.com.ezy2c.common.StringUtil;

/**
 * This class is used in both the TripProcessor and the JourneyManager. In the TripProcessor, the triggers come from the tripsDb gpslog tables (i.e. on amo5/5n). 
 * In the JourneyManager, the triggers come from the prodReadOnlyDb (i.e. from amo6/9). 
 * @author Steve
 *
 */
public class Trigger implements Comparable<Trigger>, Serializable {
	private static final Logger LOGGER = Logger.getLogger(Trigger.class.getName());
	private static final long serialVersionUID = 7593226805050267318L;
	private static final BigDecimal NINETY_DEGREES = new BigDecimal(90.0000000);
	private static final BigDecimal ONE_EIGHTY_DEGREES = new BigDecimal(180.0000000);

	/**
	 * @param commonColumnsOnly Set to true to leave out columns that aren't common to all gpslog tables
	 */
	public static String selectColumns(String tableAlias, boolean commonColumnsOnly) {
		String s;
		s = tableAlias+".`RecordID`, "+
			tableAlias+".`UnitID`, "+
			tableAlias+".`UTC_Time`, "+
			tableAlias+".`Local_Time`, "+
			tableAlias+".`Latitude`, "+
			tableAlias+".`Longitude`, "+
			tableAlias+".`Altitude`, "+
			tableAlias+".`Valid`, "+
			tableAlias+".`Speed`, "+
			tableAlias+".`Course`, "+
			tableAlias+".`Trigger`, "+
			tableAlias+".`ReceivedViaSat`, "+
			tableAlias+".`Sats`, "+
			tableAlias+".`HDOP`, "+
			tableAlias+".`Acc_X`, "+
			tableAlias+".`Acc_Y`, "+
			tableAlias+".`Acc_Z`, "+
			tableAlias+".`DCvolt`, "+
			tableAlias+".`Temp`, "+
			tableAlias+".`Battery`, "+
			tableAlias+".`Ports`, "+
			tableAlias+".`Elapsed`, "+
			tableAlias+".`ODO`, "+
			tableAlias+".`Button_ID`, "+
			tableAlias+".`Position_X`, "+
			tableAlias+".`Position_Y`, "+
			tableAlias+".`Position_Z`, "+
			tableAlias+".`Timestamp`, "+
			tableAlias+".`Geofence`, "+
			tableAlias+".`SYNCTIME`, "+
			tableAlias+".`locationtext`, "+
			tableAlias+".`sms_sent`, "+
			tableAlias+".`Analogue1`, "+
			tableAlias+".`Analogue2`, "+
			tableAlias+".`Analogue3`, "+
			tableAlias+".`Analogue4`, "+
			tableAlias+".`Analogue5`, "+
			tableAlias+".`Analogue6`, "+
			tableAlias+".`trip`, "+
			tableAlias+".`signal` ";
		if (! commonColumnsOnly) {
			s+=", "+
			tableAlias+".`TollRoad`, "+
			tableAlias+".`latzone`, "+
			tableAlias+".`lngzone` ";
		}
		return s;
	}
	/**
	 * @param commonColumnsOnly Set to true to leave out columns that aren't common to all gpslog tables
	 */
	public static Trigger parseResultSet(ResultSet rs,int i, boolean commonColumnsOnly) throws SQLException, TriggerException {
		//if (Level.DEBUG.isGreaterOrEqual(log.getEffectiveLevel()))  log.debug("Trigger.parseResultSet Started ");
		Trigger trigger = new Trigger();
		trigger.setRecordId(rs.getLong(i++));
		trigger.setUnitId(String.valueOf(rs.getInt(i++)));
		try {
		trigger.setUtcTime(rs.getTimestamp(i++));
		} catch(SQLException ex) { // Ignore Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp
			trigger.setUtcTime(new Date(0));
		}
		try {
			trigger.setLocalTime(rs.getTimestamp(i++));
		} catch(SQLException ex) { // Ignore Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp
			trigger.setLocalTime(trigger.getUtcTime());
		}
		trigger.setLatitude(rs.getBigDecimal(i++));
		trigger.setLongitude(rs.getBigDecimal(i++));
		trigger.setAltitude(rs.getBigDecimal(i++));
		trigger.setValid(rs.getInt(i++));
		trigger.setSpeed(rs.getBigDecimal(i++));
		trigger.setCourse(new BigDecimal(rs.getFloat(i++)));
		trigger.setTrigger(rs.getString(i++));
		String viaSat = rs.getString(i++);
		if (rs.wasNull())
			viaSat = null;
		trigger.setReceivedViaSat(viaSat==null?null:viaSat.toUpperCase().equals("Y")?true:false);
		trigger.setSats(rs.getInt(i++));
		trigger.setHdop(new BigDecimal(rs.getFloat(i++)));
		trigger.setAccx(rs.getBigDecimal(i++));
		trigger.setAccy(rs.getBigDecimal(i++));
		trigger.setAccz(rs.getBigDecimal(i++));
		trigger.setDcvolt(rs.getBigDecimal(i++));
		trigger.setTemp(rs.getBigDecimal(i++));
		trigger.setBattery(rs.getBigDecimal(i++));
		trigger.setPorts(rs.getInt(i++));
		trigger.setElapsed(rs.getInt(i++));
		trigger.setOdo(rs.getInt(i++));
		trigger.setButtonId(rs.getString(i++));
		trigger.setPositionx(rs.getFloat(i++));
		trigger.setPositiony(rs.getFloat(i++));
		trigger.setPositionz(rs.getFloat(i++));
		try {
			trigger.setTimestamp(rs.getTimestamp(i++));
		} catch(SQLException ex) { // Ignore Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp
			trigger.setTimestamp(new Date());
		}
		trigger.setGeofence(rs.getString(i++));
		try {
			trigger.setSyncTime(rs.getTimestamp(i++));
		} catch(SQLException ex) { // Ignore Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp
			trigger.setTimestamp(new Date());
		}
		trigger.setLocationText(rs.getString(i++));
		trigger.setSmsSent(rs.getInt(i++));
		trigger.setAnalogue1(rs.getFloat(i++));
		trigger.setAnalogue2(rs.getFloat(i++));
		trigger.setAnalogue3(rs.getFloat(i++));
		trigger.setAnalogue4(rs.getFloat(i++));
		trigger.setAnalogue5(rs.getFloat(i++));
		trigger.setAnalogue6(rs.getFloat(i++));
		trigger.setTrip(rs.getInt(i++));
		trigger.setSignal(rs.getFloat(i++));
		if (!commonColumnsOnly) {
			trigger.setTollRoad(rs.getInt(i++));
			trigger.setLatZone(rs.getInt(i++));
			trigger.setLngZone(rs.getInt(i++));
		}
		return trigger;
	}

	/**
	 * Return the columns for an INSERT INTO (<results of this method>) VALUES (<results of insertParams method>)
	 */
	public static String insertColumns(boolean isUsingOracle) {
		return insertColumns(isUsingOracle, true);
	}
	public static String insertColumns(boolean isUsingOracle, boolean includeRecordId) {
		String s;
		if (isUsingOracle) {
			if (includeRecordId) 
				s = "RecordID, ";
			else 
				s = "";
			s += "UnitID, "+
				"UTC_Time, "+
				"Local_Time, "+
				"Latitude, "+
				"Longitude, "+
				"Altitude, "+
				"Valid, "+
				"Speed, "+
				"Course, "+
				"TriggerName, "+
				"ReceivedViaSat, "+
				"Sats, "+
				"HDOP, "+
				"Acc_X, "+
				"Acc_Y, "+
				"Acc_Z, "+
				"DCvolt, "+
				"Temp, "+
				"Battery, "+
				"Ports, "+
				"Elapsed, "+
				"ODO, "+
				"Button_ID, "+
				"Position_X, "+
				"Position_Y, "+
				"Position_Z, "+
				"InsertedTimestamp, "+
				"Geofence, "+
				"SYNCTIME, "+
				"locationtext, "+
				"sms_sent, "+
				"Analogue1, "+
				"Analogue2, "+
				"Analogue3, "+
				"Analogue4, "+
				"Analogue5, "+
				"Analogue6, "+
				"trip, "+
				"signal ";
		} else {
			if (includeRecordId) 
				s = "RecordID, ";
			else
				s = "";
			s +="`UnitID`, "+
				"`UTC_Time`, "+
				"`Local_Time`, "+
				"`Latitude`, "+
				"`Longitude`, "+
				"`Altitude`, "+
				"`Valid`, "+
				"`Speed`, "+
				"`Course`, "+
				"`Trigger`, "+
				"`ReceivedViaSat`, "+
				"`Sats`, "+
				"`HDOP`, "+
				"`Acc_X`, "+
				"`Acc_Y`, "+
				"`Acc_Z`, "+
				"`DCvolt`, "+
				"`Temp`, "+
				"`Battery`, "+
				"`Ports`, "+
				"`Elapsed`, "+
				"`ODO`, "+
				"`Button_ID`, "+
				"`Position_X`, "+
				"`Position_Y`, "+
				"`Position_Z`, "+
				"`Timestamp`, "+
				"`Geofence`, "+
				"`SYNCTIME`, "+
				"`locationtext`, "+
				"`sms_sent`, "+
				"`Analogue1`, "+
				"`Analogue2`, "+
				"`Analogue3`, "+
				"`Analogue4`, "+
				"`Analogue5`, "+
				"`Analogue6`, "+
				"`trip`, "+
				"`signal` ";
		}
		return s;
	}	
	
	public void substituteParams(PreparedStatement ps, int i) throws SQLException {
		substituteParams(ps, i, true);
	}
	public int substituteParams(PreparedStatement ps, int i,boolean includeRecordId) throws SQLException {
		if (includeRecordId) 
			ps.setLong(i++, recordId);
		ps.setInt(i++, unitIdInt);
		ps.setTimestamp(i++, new Timestamp(utcTime.getTime()));
		ps.setTimestamp(i++, new Timestamp(localTime==null?utcTime.getTime():localTime.getTime()));
		ps.setBigDecimal(i++, latitude);
		ps.setBigDecimal(i++, longitude);
		ps.setBigDecimal(i++, altitude);
		ps.setInt(i++, valid);
		ps.setBigDecimal(i++, speed);
		ps.setBigDecimal(i++, course);
		ps.setString(i++, trigger);
		ps.setString(i++, receivedViaSat==null?null:receivedViaSat?"Y":"N");
		ps.setInt(i++, sats);
		ps.setBigDecimal(i++, hdop);
		ps.setBigDecimal(i++, accx);
		ps.setBigDecimal(i++, accy);
		ps.setBigDecimal(i++, accz);
		ps.setBigDecimal(i++, dcvolt);
		ps.setBigDecimal(i++, temp);
		ps.setBigDecimal(i++, battery);
		ps.setInt(i++, ports);
		ps.setInt(i++, elapsed);
		ps.setInt(i++, odo);
		ps.setString(i++, buttonId);
		ps.setFloat(i++, positionx);
		ps.setFloat(i++, positiony);
		ps.setFloat(i++, positionz);
		ps.setTimestamp(i++, new Timestamp(timestamp.getTime()));
		if (geofence == null)
			ps.setNull(i++,Types.VARCHAR);
		else
			ps.setString(i++, geofence);
		if (syncTime == null)
			ps.setNull(i++,Types.TIMESTAMP);
		else
			ps.setTimestamp(i++, new Timestamp(syncTime.getTime()));
		ps.setString(i++, locationText);
		ps.setInt(i++, smsSent);
		ps.setFloat(i++, analogue1);
		ps.setFloat(i++, analogue2);
		ps.setFloat(i++, analogue3);
		ps.setFloat(i++, analogue4);
		ps.setFloat(i++, analogue5);
		ps.setFloat(i++, analogue6);
		ps.setInt(i++, trip);
		ps.setFloat(i++, signal);
		return i;
	}
	public static String insertParams() {
		return insertParams(true);
	}
	/**
	 * Return the params for an INSERT INTO (<results of insertColumns method>) VALUES (<results of this method>)
	 */
	public static String insertParams(boolean includeRecordId) {
		String s = "";
		if (includeRecordId) {
			s += "?, ";
		}
		s+= "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ";
		return s;
	}	

	private Date utcTime;
	private String unitId;
	private int unitIdInt;
	private String trigger;
	private Boolean receivedViaSat;
	private BigDecimal latitude;
	private BigDecimal longitude;
	private int odo;
	private int driverId;
	private String driverIdFamilyName;		// Last two chars of button Id is family name: FC keypad for private time, FD keypad for work time, FE dcu, 01 is dallas key, 28 is temperature, 10 is normal trailer id
	private int valid;
	private BigDecimal speed;
	private BigDecimal course;
	private int sats;
	private BigDecimal hdop;
	private int trip;
	private String locationText;
	private Date timestamp;
	
	private long recordId;
	private Date localTime;
	private BigDecimal altitude;
	private BigDecimal accx;
	private BigDecimal accy;
	private BigDecimal accz;
	private BigDecimal dcvolt;
	private BigDecimal temp;
	private BigDecimal battery;
	private int ports;
	private int elapsed;
	private String buttonId;
	private float positionx;
	private float positiony;
	private float positionz;
	private String geofence;
	private Date syncTime;
	private int smsSent;
	private float analogue1;
	private float analogue2;
	private float analogue3;
	private float analogue4;
	private float analogue5;
	private float analogue6;

	private int tollRoad;
	private int latZone;
	private int lngZone;
	private float signal;
	
	public Trigger() {
		
	}
	public Trigger(JSONObject json) throws ParseException, JSONException {
		recordId = json.getLong("recordId");
		utcTime = DateUtil.convertStringToTimestamp(json.getString("utcTime"));
		unitIdInt = json.getInt("unitId");
		unitId = String.valueOf(unitIdInt);
		trigger = json.getString("trigger");
		String s = json.getString("receivedViaSat");
		if ("Y".equals(s))
			receivedViaSat = true;
		else
			receivedViaSat = false;
		latitude = new BigDecimal(json.getString("latitude"));
		longitude = new BigDecimal(json.getString("longitude"));
		odo = json.getInt("odo");
		driverId = json.getInt("driverId");
		if (json.has("driverIdFamilyName"))
			driverIdFamilyName = json.getString("driverIdFamilyName");
		valid = json.getInt("valid");
		speed = new BigDecimal(json.getString("speed"));
		course = new BigDecimal(json.getString("course"));
		sats = json.getInt("sats");
		hdop = new BigDecimal(json.getString("hdop"));
		locationText = json.getString("locationText");
		timestamp = DateUtil.convertStringToTimestamp(json.getString("timestamp"));
		altitude = new BigDecimal(json.getString("altitude"));
		accx = new BigDecimal(json.getString("accx"));
		accy = new BigDecimal(json.getString("accy"));
		accz = new BigDecimal(json.getString("accz"));
		dcvolt = new BigDecimal(json.getString("dcvolt"));
		temp = new BigDecimal(json.getString("temp"));
		battery = new BigDecimal(json.getString("battery"));
		ports = json.getInt("ports");
		elapsed = json.getInt("elapsed");
		buttonId = json.getString("buttonId");
		positionx = new Float(json.getString("positionx"));
		positiony = new Float(json.getString("positiony"));
		positionz = new Float(json.getString("positionz"));
		geofence = json.getString("geofence");
		syncTime = DateUtil.convertStringToTimestamp(json.getString("syncTime"));
		smsSent = json.getInt("smsSent");
		analogue1 = new Float(json.getString("analogue1"));
		analogue2 = new Float(json.getString("analogue2"));
		analogue3 = new Float(json.getString("analogue3"));
		analogue4 = new Float(json.getString("analogue4"));
		analogue5 = new Float(json.getString("analogue5"));
		analogue6 = new Float(json.getString("analogue6"));
		signal = new Float(json.getString("signal"));
		if (json.has("tollRoad"))
			tollRoad = json.getInt("tollRoad");
		if (json.has("latZone"))
			latZone = json.getInt("latZone");
		if (json.has("lngZone"))
			lngZone = json.getInt("lngZone");
	}	

	public String toJSONString() throws JSONException {
		JSONObject json = new JSONObject();
		json.put("recordId", recordId);
		json.put("utcTime", DateUtil.convertDateAndTimeToString(utcTime));
		json.put("unitId", unitIdInt);
		json.put("trigger", trigger);
		json.put("receivedViaSat", receivedViaSat==null?"N":"Y");
		json.put("latitude", String.valueOf(latitude));
		json.put("longitude", String.valueOf(longitude));
		json.put("odo", odo);
		json.put("driverId", driverId);
		json.put("driverIdFamilyName", driverIdFamilyName);
		json.put("valid", valid);
		json.put("speed", String.valueOf(speed));
		json.put("course", String.valueOf(course));
		json.put("sats", sats);
		json.put("hdop", String.valueOf(hdop));
		json.put("locationText", locationText);
		json.put("timestamp", DateUtil.convertDateAndTimeToString(timestamp));
		json.put("altitude", String.valueOf(altitude));
		json.put("accx", String.valueOf(accx));
		json.put("accy", String.valueOf(accy));
		json.put("accz", String.valueOf(accz));
		json.put("dcvolt", String.valueOf(dcvolt));
		json.put("temp", String.valueOf(temp));
		json.put("battery", String.valueOf(battery));
		json.put("ports", ports);
		json.put("elapsed", elapsed);
		json.put("buttonId", buttonId);
		json.put("positionx", String.valueOf(positionx));
		json.put("positiony", String.valueOf(positiony));
		json.put("positionz", String.valueOf(positionz));
		json.put("geofence", geofence);
		json.put("syncTime", DateUtil.convertDateAndTimeToString(syncTime));
		json.put("smsSent", smsSent);
		json.put("analogue1", String.valueOf(analogue1));
		json.put("analogue2", String.valueOf(analogue2));
		json.put("analogue3", String.valueOf(analogue3));
		json.put("analogue4", String.valueOf(analogue4));
		json.put("analogue5", String.valueOf(analogue5));
		json.put("analogue6", String.valueOf(analogue6));
		json.put("signal", String.valueOf(signal));
		return json.toString();
	}				
	public boolean isAfter(Date when) {
		return utcTime != null && utcTime.after(when);
	}
	/**
	 * Comparison based on utcTime, unitId, recordId (sometimes two triggers for the same unit turn up at the same time so order it on the order the server received it (recordId) 
	 */
	@Override
	public int compareTo(Trigger o) {
		int result = 0;
		if (utcTime == null && o.utcTime != null)
			result = -1;
		else if (utcTime == null && o.utcTime == null)
			result = 0;
		else 
			result =  utcTime.compareTo(o.utcTime);
		if (result == 0) {
			if (unitIdInt > o.unitIdInt)
				result = +1;
			else if (unitIdInt < o.unitIdInt)
				result = -1;
		}
		if (result == 0) {
			if (recordId < o.recordId)
				result = -1;
			else if (recordId > o.recordId)
				result = +1;
		}
		return result;
	}
	
	
	public String toString() {
		return toString(0);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((trigger == null) ? 0 : trigger.hashCode());
		result = prime * result + ((unitId == null) ? 0 : unitId.hashCode());
		result = prime * result + ((utcTime == null) ? 0 : utcTime.hashCode());
		return result;
	}
	/**
	 * Equal if they are the same unit, time, and trigger name
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Trigger other = (Trigger) obj;
		if (trigger == null) {
			if (other.trigger != null)
				return false;
		} else if (!trigger.equals(other.trigger))
			return false;
		if (unitId == null) {
			if (other.unitId != null)
				return false;
		} else if (!unitId.equals(other.unitId))
			return false;
		if (utcTime == null) {
			if (other.utcTime != null)
				return false;
		} else if (!utcTime.equals(other.utcTime))
			return false;
		return true;
	}
	public String toString(int indent) {
		StringBuilder s = new StringBuilder();
		s.append(StringUtil.spaces(indent));
		s.append("<Trigger utcTime='");
		s.append(utcTime);
		s.append("' recordId='");
		s.append(recordId);
		s.append("' unitId=");
		s.append(unitId);
		s.append(" trigger='");
		s.append(trigger);
		s.append("' receivedViaSat='");
		s.append(receivedViaSat);
		s.append("' locationText='");
		s.append(locationText);
		s.append("'/>\n");
		return s.toString();
	}
	/**
	 * Return true if this is a satellite trigger
	 */
	public boolean isSatTrigger() {
		return receivedViaSat != null && receivedViaSat==true;
	}
	/**
	 * Sat triggers are not handled
	 * @return	true if this is one of the triggers that affects trip boundaries
	 */
	public boolean isTripTrigger() {
		return  isNormal() || isHealthCheck() || isIgnitionOnOrSatIgnOn() || isIgnitionOffOrSatIgnOff() || isSpeeding();
	}

	public boolean isIgnitionOn() {
		return trigger != null && (trigger.startsWith("Ignition On") || trigger.startsWith("Movement"));
	}

	public boolean isIgnitionOnOrSatIgnOn() {
		return isIgnitionOn() || isSatIgnOn();
	}

	public boolean isSatIgnOn() {
		return trigger != null && trigger.startsWith("SAT ign On");
	}

	public boolean isReset() {
		return trigger != null && (trigger.indexOf("Reset") > -1);
	}
	public void changeTriggerToNormal() {
		trigger = "Normal";
	}
	/**
	 * Sat triggers are not handled yet
	 */
	public boolean isHealthCheck() {
		return trigger != null && trigger.startsWith("Health Check");
	}
	/**
	 * 1 => valid
	 * 9 => Don't trust location or speed 
	 * 8 => trust location but not speed
	 * 0 => double asterisk => Invalid - don't trust speed or location 
     *                         ODO is okay but it might be a bit shorter than it should be as GPS was lost at some point
     * Omni MT-2000 units sometimes report a lat,long of 90.0000000,180.0000000, which is invalid although the valid flag comes through as 1.
	 */
	public boolean isLocationValid() {
		return (valid == 1 || valid == 8) && ! (latitude.equals(NINETY_DEGREES) && longitude.equals(ONE_EIGHTY_DEGREES));
	}
	/**
	 * 1 => valid
	 * 9 => Don't trust location or speed 
	 * 8 => trust location but not speed
	 * 0 => double asterisk => Invalid - don't trust speed or location 
     *                         ODO is okay but it might be a bit shorter than it should be as GPS was lost at some point
     * Omni MT-2000 units sometimes report a lat,long of 90.0000000,180.0000000, which is invalid although the valid flag comes through as 1.
	 */
	public boolean isSpeedValid() {
		return valid == 1;
	}
	/**
	 * 1 => valid
	 * 9 => Don't trust location or speed 
	 * 8 => trust location but not speed
	 * 0 => double asterisk => Invalid - don't trust speed or location 
     *                         ODO is okay but it might be a bit shorter than it should be as GPS was lost at some point
	 */
	public boolean isOdoValid() {
		return valid == 1 || valid == 0;
	}
	/**
	 * Return true if the lat and long are non zero and the position is valid
	 */
	public boolean isPositionValid() {
		if (isLocationValid() && latitude != null && latitude != BigDecimal.ZERO && longitude != null && longitude != BigDecimal.ZERO)
			return true;
		return false;
	}
	
	public boolean isIgnitionOff() {
		return trigger != null && (trigger.startsWith("Ignition Off") || trigger.startsWith("Stationary"));
	}
	
	public boolean isIgnitionOffOrSatIgnOff() {
		return isIgnitionOff() || isSatIgnOff();
	}
	
	public boolean isSatIgnOff() {
		return trigger != null && trigger.startsWith("SAT ign Off");
	}
	
	/**
	 * Sat triggers are not handled yet
	 */
	public boolean isNormal() {
		return trigger != null && trigger.startsWith("Normal");
	}
	/**
	 * Sat triggers are not handled yet
	 */
	public boolean isSpeeding() {
		return trigger != null && trigger.startsWith("Speeding");
	}
	public boolean isMovementStop() {
		return trigger != null && trigger.startsWith("Movement Stop");
	}
	public boolean isMovementStart() {
		return trigger != null && trigger.startsWith("Movement Start");
	}
	/**
	 * See if there is a failure in the GPS Module
	 */
	public boolean isGPSStopped() {
		return trigger != null && trigger.startsWith("GPS Stopped");
	}
	/**
	 * Return true if this trigger indicates the accelerometers are not set
	 */
	public boolean isAccelerometersNotSet() {
		return trigger != null && (trigger.startsWith("Suppress Acc Not Set") || trigger.startsWith("Acc Not Set"));
	}
	/**
	 * Return true if the date of this trigger is before the given date
	 * @param when
	 * @return
	 */
	public boolean before(Date when) {
		return utcTime == null || utcTime.before(when);
	}
	
	/**
	 * Return true if the date of this trigger is before or the same as the given date
	 * @param when
	 * @return
	 */
	public boolean beforeOrSameAs(Date when) {
		return ! after(when);
	}
	
	/**
	 * Return true if the date of this trigger is after the given date
	 * @param when
	 * @return
	 */
	public boolean after(Date when) {
		return utcTime == null || utcTime.after(when);
	}
	/**
	 * Return true if the date of this trigger is after or the same as the given date
	 * @param when
	 * @return
	 */
	public boolean afterOrSameAs(Date when) {
		return ! before(when);
	}
	
	/**
	 * Delete this trigger from the tripprocessor.gpslog0x0 table
	 */
	public void delete(Connection tripsDb, String gpslogTableName) {
		LOGGER.log(Level.INFO,"Trigger.delete: Started");
		PreparedStatement ps = null;
		String sql = null;

		try {
			// MySQL Only
			sql = "DELETE FROM tripprocessor."+gpslogTableName+" "
			    + "WHERE recordid = ? ";
			
			ps = tripsDb.prepareStatement(sql);
			int i = 1;
			ps.setLong(i++,recordId);
			int numRowsDeleted = ps.executeUpdate();
			if (numRowsDeleted != 1) {
				LOGGER.log(Level.INFO,"WARNING: Trigger.delete: Failed to delete the trigger with record id "+recordId+" from gpslog "+gpslogTableName+" using sql "+sql+": numRowsDeleted should be 1 but it is "+numRowsDeleted);
			}
		} catch (SQLException ex) {
			LOGGER.log(Level.WARNING,"WARNING: Trigger.delete: Failed to delete the trigger with record id "+recordId+" from gpslog "+gpslogTableName+" using sql "+sql+": SQLException: "+ ex);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
				}
				ps = null;
			}
		}	
		LOGGER.log(Level.INFO,"Trigger.delete: Finished");
	}


	public int getTrip() {
		return trip;
	}

	public void setTrip(int trip) {
		this.trip = trip;
	}

	public String getLocationText() {
		return locationText;
	}

	public void setLocationText(String locationText) {
		this.locationText = locationText;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public int getValid() {
		return valid;
	}

	public void setValid(int valid) {
		this.valid = valid;
	}

	public BigDecimal getCourse() {
		return course;
	}

	public void setCourse(BigDecimal course) {
		this.course = course;
	}

	public int getSats() {
		return sats;
	}

	public void setSats(int sats) {
		this.sats = sats;
	}

	public BigDecimal getHdop() {
		return hdop;
	}

	public void setHdop(BigDecimal hdop) {
		this.hdop = hdop;
	}

	public Date getUtcTime() {
		return utcTime;
	}

	public void setUtcTime(Date utcTime) {
		this.utcTime = utcTime;
	}

	public String getUnitId() {
		return unitId;
	}

	public int getUnitIdInt() {
		return unitIdInt;
	}
	public void setUnitId(String unitId) throws TriggerException {
		this.unitId = unitId;
		try {
			this.unitIdInt = Integer.parseInt(unitId);
		} catch (NumberFormatException ex) {
			LOGGER.log(Level.SEVERE, "ERROR: Trigger.setUnitId: Unable to convert unitId value '"+unitId+"' from string unitId '"+unitId+"'into an int NumberFormatException "+ex);
			throw new TriggerException("Unable to convert unitId value '"+unitId+"' from string unitId '"+unitId+"'into an int: caught NumberFormatException",ex);
		}
	}

	public String getTrigger() {
		return trigger;
	}

	public void setTrigger(String trigger) {
		this.trigger = trigger;
	}

	public BigDecimal getLatitude() {
		return latitude;
	}

	public void setLatitude(BigDecimal latitude) {
		this.latitude = latitude;
	}

	public BigDecimal getLongitude() {
		return longitude;
	}

	public void setLongitude(BigDecimal longitude) {
		this.longitude = longitude;
	}

	public int getOdo() {
		return odo;
	}

	public void setOdo(int odo) {
		this.odo = odo;
	}

	public int getDriverId() {
		return driverId;
	}

	public void setDriverId(int driverId) {
		this.driverId = driverId;
	}

	public String getDriverIdFamilyName() {
		return driverIdFamilyName;
	}

	public void setDriverIdFamilyName(String driverIdFamilyName) {
		this.driverIdFamilyName = driverIdFamilyName;
	}

	public long getRecordId() {
		return recordId;
	}

	public void setRecordId(long recordId) {
		this.recordId = recordId;
	}

	public Date getLocalTime() {
		return localTime;
	}

	public void setLocalTime(Date localTime) {
		this.localTime = localTime;
	}


	public int getPorts() {
		return ports;
	}

	public void setPorts(int ports) {
		this.ports = ports;
	}

	public int getElapsed() {
		return elapsed;
	}

	public void setElapsed(int elapsed) {
		this.elapsed = elapsed;
	}

	public String getButtonId() {
		return buttonId;
	}

	public void setButtonId(String buttonId) {
		this.buttonId = buttonId;
	}

	public float getPositionx() {
		return positionx;
	}

	public void setPositionx(float positionx) {
		this.positionx = positionx;
	}

	public float getPositiony() {
		return positiony;
	}

	public void setPositiony(float positiony) {
		this.positiony = positiony;
	}

	public float getPositionz() {
		return positionz;
	}

	public void setPositionz(float positionz) {
		this.positionz = positionz;
	}

	public String getGeofence() {
		return geofence;
	}

	public void setGeofence(String geofence) {
		this.geofence = geofence;
	}

	public Date getSyncTime() {
		return syncTime;
	}

	public void setSyncTime(Date syncTime) {
		this.syncTime = syncTime;
	}

	public int getSmsSent() {
		return smsSent;
	}

	public void setSmsSent(int smsSent) {
		this.smsSent = smsSent;
	}

	public float getAnalogue1() {
		return analogue1;
	}

	public void setAnalogue1(float analogue1) {
		this.analogue1 = analogue1;
	}

	public float getAnalogue2() {
		return analogue2;
	}

	public void setAnalogue2(float analogue2) {
		this.analogue2 = analogue2;
	}

	public float getAnalogue3() {
		return analogue3;
	}

	public void setAnalogue3(float analogue3) {
		this.analogue3 = analogue3;
	}

	public float getAnalogue4() {
		return analogue4;
	}

	public void setAnalogue4(float analogue4) {
		this.analogue4 = analogue4;
	}

	public float getAnalogue5() {
		return analogue5;
	}

	public void setAnalogue5(float analogue5) {
		this.analogue5 = analogue5;
	}

	public float getAnalogue6() {
		return analogue6;
	}

	public void setAnalogue6(float analogue6) {
		this.analogue6 = analogue6;
	}
	public int getSpeedZone() {
		return (int) analogue5;
	}

	public void setSpeedZone(int speedZone) {
		this.analogue5 = (float) speedZone;
	}
	
	public int getTollRoad() {
		return tollRoad;
	}

	public void setTollRoad(int tollRoad) {
		this.tollRoad = tollRoad;
	}

	public int getLatZone() {
		return latZone;
	}

	public void setLatZone(int latZone) {
		this.latZone = latZone;
	}

	public int getLngZone() {
		return lngZone;
	}

	public void setLngZone(int lngZone) {
		this.lngZone = lngZone;
	}

	public float getSignal() {
		return signal;
	}

	public void setSignal(float signal) {
		this.signal = signal;
	}

	public BigDecimal getSpeed() {
		return speed;
	}

	public void setSpeed(BigDecimal speed) {
		this.speed = speed;
	}

	public BigDecimal getAltitude() {
		return altitude;
	}

	public void setAltitude(BigDecimal altitude) {
		this.altitude = altitude;
	}

	public BigDecimal getAccx() {
		return accx;
	}

	public void setAccx(BigDecimal accx) {
		this.accx = accx;
	}

	public BigDecimal getAccy() {
		return accy;
	}

	public void setAccy(BigDecimal accy) {
		this.accy = accy;
	}

	public BigDecimal getAccz() {
		return accz;
	}

	public void setAccz(BigDecimal accz) {
		this.accz = accz;
	}

	public BigDecimal getDcvolt() {
		return dcvolt;
	}

	public void setDcvolt(BigDecimal dcvolt) {
		this.dcvolt = dcvolt;
	}

	public BigDecimal getTemp() {
		return temp;
	}

	public void setTemp(BigDecimal temp) {
		this.temp = temp;
	}

	public BigDecimal getBattery() {
		return battery;
	}

	public void setBattery(BigDecimal battery) {
		this.battery = battery;
	}
	public void setReceivedViaSat(Boolean receivedViaSat) {
		this.receivedViaSat = receivedViaSat;
	}
}
