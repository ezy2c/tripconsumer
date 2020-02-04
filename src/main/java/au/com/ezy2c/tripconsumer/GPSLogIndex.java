package au.com.ezy2c.tripconsumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.stereotype.Component;

@Component
public class GPSLogIndex {
	private static final Logger LOGGER = Logger.getLogger(GPSLogIndex.class.getName());    

	class GPSLogIndexEntry {
		private int minUnitId;
		private int maxUnitId;
		private String gpslogTableName;
		
		public GPSLogIndexEntry() {
		}

		public int getMinUnitId() {
			return minUnitId;
		}

		public void setMinUnitId(int minUnitId) {
			this.minUnitId = minUnitId;
		}

		public int getMaxUnitId() {
			return maxUnitId;
		}

		public void setMaxUnitId(int maxUnitId) {
			this.maxUnitId = maxUnitId;
		}

		public String getGpslogTableName() {
			return gpslogTableName;
		}

		public void setGpslogTableName(String gpslogTableName) {
			this.gpslogTableName = gpslogTableName;
		}

	}
	
	private List<GPSLogIndexEntry> entries = null;
	
	public GPSLogIndex() {
	}
	
	private void loadEntries(Connection c) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "SELECT minUnitID, maxUnitID, gpslogTableName FROM gpslogIndex ORDER BY minUnitId ";
		entries = new ArrayList<>();
		try {
			ps = c.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				GPSLogIndexEntry entry = new GPSLogIndexEntry();
				int i = 1;
				entry.setMinUnitId(rs.getInt(i++));
				entry.setMaxUnitId(rs.getInt(i++));
				entry.setGpslogTableName(rs.getString(i++));
				entries.add(entry);
			}
		} catch (SQLException ex) {
			String msg = "Unable to load the gpslogIndex using query "+sql+" : SQLException "+ex.getMessage();
			LOGGER.log(Level.SEVERE,msg);
			throw ex;
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Throwable th) {
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Throwable th) {
				}
			}
		}
	}
	/**
	 * Returns the gpslog table from gpslogIndex corresponding to the given unitId
	 * @param unitId
	 * @return null if not found
	 */
	public String findGpslogTableName(Connection c, int unitId) throws SQLException {
		if (entries == null)
			loadEntries(c);
		String gpslogTableName = null;
		for (GPSLogIndexEntry entry : entries) {
			if (unitId >= entry.getMinUnitId() && unitId <= entry.getMaxUnitId()) {
				return entry.getGpslogTableName();
			}
		}
		return gpslogTableName;
	}
}
