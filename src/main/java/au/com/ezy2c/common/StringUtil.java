package au.com.ezy2c.common;

public class StringUtil {
	public static String spaces(int n) {
		StringBuilder s = new StringBuilder();
		for (int i = 0; i < n; i++)
			s.append(" ");
		return s.toString();
	}
	
	public static boolean isBlankString(String s) {
		return s == null || s.trim().equals("");
	}
	public static boolean isPositiveInteger(String s) {
		if (isBlankString(s))
			return false;
		for (char c : s.toCharArray()) {
			if (! Character.isDigit(c)) {
				return false;
			}
		}
		return true;
	}
}
