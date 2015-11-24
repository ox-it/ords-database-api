/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.ox.it.ords.api.database.queries;

import java.util.ResourceBundle;
import java.util.Locale;

/**
 *
 * @author mark
 */
public class OrdsLangBundle {

    private Boolean debug;
    private ResourceBundle bundle;

    public OrdsLangBundle(String basename, Locale locale, Boolean debug) {
        bundle = ResourceBundle.getBundle(basename, locale);
        this.debug = debug;
    }
    
    public String getString(String key) {
        String langstring = "";
        if (debug) {
            langstring += "["+key+"] ";
        }
        langstring += bundle.getString(key);
        langstring = langstring.replace("\n\n", "</p><p>");
        langstring = langstring.replace("\n", "<br />");
        return langstring;
    }
    
}
