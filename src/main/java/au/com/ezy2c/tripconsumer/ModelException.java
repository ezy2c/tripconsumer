package au.com.ezy2c.tripconsumer;

import java.io.Serializable;

public class ModelException extends Exception implements Serializable {
	private static final long serialVersionUID = 2846025439014834063L;

	private String message;
	private Throwable ex;
	
	public ModelException(String message) {
		this.message = message;
		this.ex = null;
	}
	
	public ModelException(String message, Throwable ex) {
		this.message = message;
		this.ex = ex;
	}
	
	public String toString() {
		return message+(ex==null?"":(": "+ex.toString()));
	}
}
