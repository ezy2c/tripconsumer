package au.com.ezy2c.tripconsumer;

import org.json.JSONException;
import org.json.JSONObject;

public class TriggerKey {
	private int unitId;				// This is what the consumers filter on
	private String serverId;		// Along with recordId makes the record unique
	private long recordId;			// Along with serverId makes the record unique
	
	public TriggerKey(int unitId, String serverId, long recordId) {
		this.unitId = unitId;
		this.serverId = serverId;
		this.recordId = recordId;
	}
	public TriggerKey(JSONObject json) throws JSONException {
		this.unitId = json.getInt("unitId");
		this.serverId = json.getString("serverId");
		this.recordId = json.getLong("recordId");
	}
	public String toJSONString() throws JSONException {
		JSONObject json = new JSONObject();
		json.put("unitId", unitId);
		json.put("serverId", serverId);
		json.put("recordId", recordId);
		return json.toString();
	}
	public long getRecordId() {
		return recordId;
	}
	public int getUnitId() {
		return unitId;
	}
	public String getServerId() {
		return serverId;
	}
}


