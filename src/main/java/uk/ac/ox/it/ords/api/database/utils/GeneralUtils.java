package uk.ac.ox.it.ords.api.database.utils;

public class GeneralUtils {

	public static String implode(String separator, String... data) {
	    StringBuilder sb = new StringBuilder();
	    for (int i = 0; i < data.length - 1; i++) {
	    //data.length - 1 => to not add separator at the end
	        if (!data[i].matches(" *")) {//empty string are ""; " "; "  "; and so on
	            sb.append(data[i]);
	            sb.append(separator);
	        }
	    }
	    sb.append(data[data.length - 1].trim());
	    return sb.toString();
	}
}
