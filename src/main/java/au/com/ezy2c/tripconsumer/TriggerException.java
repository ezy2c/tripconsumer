package au.com.ezy2c.tripconsumer;

import java.io.Serializable;

public class TriggerException extends ModelException implements Serializable {
	private static final long serialVersionUID = -8955253377635308352L;

	public TriggerException(String message) {
		super(message);
	}
	public TriggerException(String message, Exception ex) {
		super(message,ex);
	}
}
