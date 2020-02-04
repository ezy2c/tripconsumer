package au.com.ezy2c.common;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DateUtil {
	private static final Logger LOGGER = Logger.getLogger(DateUtil.class.getName());    
	public static String START_OF_TIME = "1970-01-01 00:00:00";
	public static String END_OF_TIME   = "2199-12-31 23:59:59";
	
	public static Date convertStringToDate(String dateString) {
      try {
          Date date = new SimpleDateFormat("yyyy-MM-dd").parse(dateString);
          //log.debug("DateUtil.convert: Original string: " + dateString+" Parsed date: " + date.toString());
          return date;
      }
      catch(ParseException ex) {
    	  LOGGER.log(Level.SEVERE,"DateUtil.convertStringToDate: ERROR: could not parse date in string '" + dateString + "' - returning null - ParseException: ",ex);
          return null;
      }
	}
	public static Timestamp convertStringToTimestamp(String dateString) throws NumberFormatException, ParseException {
	       try {
	           Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateString);
	           //log.debug("DateUtil.convert: Original string: " + dateString+" Parsed date: " + date.toString());
	           return new Timestamp(date.getTime());
	       }
	       catch(ParseException ex) {
	    	   LOGGER.log(Level.SEVERE,"DateUtil.convertStringToTimestamp: ERROR: could not parse date in string '" + dateString + "' - returning null - ParseException: "+ex,ex);
	           throw ex;
	       } catch(NumberFormatException ex) {
	    	   LOGGER.log(Level.SEVERE,"DateUtil.convertStringToTimestamp: ERROR: could not parse date in string '" + dateString + "' - returning null - NumberFormatException: "+ex,ex);
	           throw ex;
	       }
		}
	public static Timestamp convertBigDataStringToTimestamp(String dateString) throws NumberFormatException, ParseException {
		try {
			Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").parse(dateString);
			//log.debug("DateUtil.convert: Original string: " + dateString+" Parsed date: " + date.toString());
			return new Timestamp(date.getTime());
		}	
		catch(ParseException ex) {
			LOGGER.log(Level.SEVERE,"DateUtil.convertStringToTimestamp: ERROR: could not parse date in string '" + dateString + "' - returning null - ParseException: "+ex,ex);
			throw ex;
		} catch(NumberFormatException ex) {
			LOGGER.log(Level.SEVERE,"DateUtil.convertStringToTimestamp: ERROR: could not parse date in string '" + dateString + "' - returning null - NumberFormatException: "+ex,ex);
			throw ex;
		}
	}
	public static Timestamp convertLongBigDataStringToTimestamp(String dateString) throws NumberFormatException, ParseException {
		try {
			Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSX").parse(dateString);
			//log.debug("DateUtil.convert: Original string: " + dateString+" Parsed date: " + date.toString());
			return new Timestamp(date.getTime());
		}	
		catch(ParseException ex) {
			LOGGER.log(Level.SEVERE,"DateUtil.convertLongBigDataStringToTimestamp: ERROR: could not parse date in string '" + dateString + "' - returning null - ParseException: "+ex,ex);
			throw ex;
		} catch(NumberFormatException ex) {
			LOGGER.log(Level.SEVERE,"DateUtil.convertLongBigDataStringToTimestamp: ERROR: could not parse date in string '" + dateString + "' - returning null - NumberFormatException: "+ex,ex);
			throw ex;
		}
	}

	public static String convertDateToString(Date date) {
		return new SimpleDateFormat("yyyy-MM-dd").format(date);
	}
	
	public static String convertDateToStringForFileName(Date date) {
		return new SimpleDateFormat("yyyyMMdd").format(date);
	}
	
	public static String convertDateAndTimeToString(Date date) {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
	}
	public static String convertDateAndTimeToStringForLogFile(Date date) {
		return new SimpleDateFormat("yyyyMMddHHmm").format(date);
	}
	public static Date getStartOfTime() throws ParseException, NumberFormatException {
		return convertStringToTimestamp(START_OF_TIME);
	}
	public static Boolean isStartOfTime(Date d) throws ParseException, NumberFormatException  {
		return d.compareTo(getStartOfTime())==0;
	}
	public static Date getEndOfTime() throws ParseException, NumberFormatException {
		return convertStringToTimestamp(END_OF_TIME);
	}
	/**
	 * Returns true if the given dates are with the given number of hours of each other
	 * @param a
	 * @param b
	 * @param hours
	 * @return
	 */
	public static Boolean isWithinHours(Date a, Date b, long hours) {
		long absDiff = Math.abs(a.getTime()-b.getTime());
		Boolean diff = (absDiff <= (hours * 3600000L));
		return diff;
	}
	/**
	 * Returns true if the given dates are with the given number of minutes of each other
	 * @param a
	 * @param b
	 * @param hours
	 * @return
	 */
	public static Boolean isWithinMinutes(Date a, Date b, long minutes) {
		long absDiff = Math.abs(a.getTime()-b.getTime());
		Boolean diff = (absDiff <= (minutes * 60000L));
		return diff;
	}
	/**
	 * Returns true if the given dates are with the given number of seconds of each other
	 * @param a in milliseconds since epoch
	 * @param b in millseconds since epoch
	 * @param seconds
	 * @return true if a and b are within (i.e. +/-) seconds of each other
	 */
	public static Boolean isWithinSeconds(long a, long b, long seconds) {
		long absDiff = Math.abs(a-b);
		Boolean diff = (absDiff <= (seconds * 1000L));
		//System.out.println("isWithinSeconds: Given "+a+" and "+b+" diff is "+absDiff+" returning "+diff);
		return diff;
	}
	/**
	 * Returns true if the two date times are on the same day
	 */
	public static Boolean sameDayAs(Date a, Date b) {
		Calendar cala = Calendar.getInstance();
		Calendar calb = Calendar.getInstance();
		cala.setTime(a);
		calb.setTime(b);
		Boolean sameDay = cala.get(Calendar.YEAR) == calb.get(Calendar.YEAR) &&
                  cala.get(Calendar.DAY_OF_YEAR) == calb.get(Calendar.DAY_OF_YEAR);
		return sameDay;
	}
	
	/**
	 * Return a timestamp the given number of days before the given date
	 */
	public static Timestamp subtractDays(Date d, int numDays) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		cal.add(Calendar.DAY_OF_YEAR, -numDays);
		Timestamp t = new Timestamp(cal.getTime().getTime());
		return t;
	}
		
	public static Timestamp now() {
		return new Timestamp(new Date().getTime());
	}
	public static long nowUtcTime() {
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		return c.getTimeInMillis();
	}
	
	/**
	 * Return a String containing the number of hours, minutes, and seconds behind the given time is 
	 * @param time
	 * @return
	 */
	public static String durationFromNow(long time) {
		if (time > 0) {
			StringBuilder sb = new StringBuilder();
			long currentTimeInSecs = new Date().getTime()/1000l - 10*3600;
			long timeInSecs = time/1000l;
			long diffInSecs = currentTimeInSecs - timeInSecs;
			long diffInMinutes = diffInSecs / 60;
			long diffInHours = diffInMinutes / 60;
			diffInMinutes -= diffInHours * 60;
			diffInSecs -= diffInMinutes * 60 + diffInHours * 3600;
			if (diffInHours > 0l) {
				sb.append(diffInHours);
				sb.append(" hours ");
			}
			if (diffInHours > 0l || diffInMinutes > 0l) {
				sb.append(diffInMinutes);
				sb.append(" minutes ");
			}
			sb.append(diffInSecs);
			sb.append(" seconds ");
			return sb.toString();
		} else {
			return "";
		}
	}
	
	public static void main(String args[]) {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, 2012);
		cal.set(Calendar.MONTH, 10);
		cal.set(Calendar.DAY_OF_MONTH, 20);
		cal.set(Calendar.HOUR_OF_DAY, 01);
		cal.set(Calendar.MINUTE,55);
		cal.set(Calendar.SECOND, 30);
		cal.set(Calendar.MILLISECOND, 0);
		Date d = cal.getTime();
		System.out.println("Date "+convertDateAndTimeToString(d)+" is "+d.getTime());
		Calendar cal2 = Calendar.getInstance();
		cal2.add(Calendar.HOUR,-3);
		cal2.add(Calendar.MINUTE, -2);
		cal2.add(Calendar.SECOND, -1);
		System.out.println("Difference from "+cal2.getTime()+" is "+DateUtil.durationFromNow(cal2.getTimeInMillis()));
	}
}
