package au.com.ezy2c.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBUtil {
	public static Connection mySQLConnect(String url) throws DBConnectException {
		Connection conn = null;
		try {
			//System.err.println("DBUtil.connect(dbName): Connecting to url '"+URL_START+dbName+URL_PARAMS+"'");
			conn = DriverManager.getConnection(url);
			return conn;
		
		} catch (SQLException ex) {
			// handle any errors
			System.err.println("DBUtil.connect: Unable to connect using URL '"+url+"'");
			System.err.println("DBUtil.connect: SQLException: " + ex.getMessage());
			System.err.println("DBUtil.connect: SQLState: " + ex.getSQLState());
			System.err.println("DBUtil.connect: VendorError: " + ex.getErrorCode());
			ex.printStackTrace(System.err);
			throw new DBConnectException("DBUtil.connect: Unable to connect to the database: ",ex);
		}
	}
	

}
