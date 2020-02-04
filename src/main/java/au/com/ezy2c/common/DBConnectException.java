package au.com.ezy2c.common;

import java.io.Serializable;

public class DBConnectException extends Exception implements Serializable {
	private static final long serialVersionUID = -5241554853174409769L;
		
	public DBConnectException(String message) {
		super(message);
	}
	public DBConnectException(String message, Exception ex) {
		super(message,ex);
	}
}
