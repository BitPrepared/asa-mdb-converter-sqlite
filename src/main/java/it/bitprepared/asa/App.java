package it.bitprepared.asa;

import it.bitprepared.asa.exporter.AccessExporter;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) 
    {
    	args = new String[1];
    	args[0] = "/Users/yoghi/Desktop/ultimo materiale/video/"; //estrazione_capiPiacenza.mdb
    	
    	if (args.length != 1) {
            System.out.println(String.format("Usage: %s <access file directory>", App.class.getName()));
            System.exit(1);
        }
    	
    	File[] files = new File(args[0]).listFiles();
    	for (File file : files) {
    		String name = file.getName();
    		if ( name.endsWith(".mdb") ) {
	    		System.out.println("Try decode "+name);
	    		try {
	    	        /* Load the SQLite driver */
	    	        Class.forName("org.sqlite.JDBC");
	    	
	    	        /* Do the export */
	    	        
	    	        DatabaseBuilder dbuilder = new DatabaseBuilder(file);
	    	        dbuilder.setReadOnly(true);
	    	        Database db = dbuilder.open();
	    	        final AccessExporter exporter = new AccessExporter(db);
	    	        final Connection jdbc = DriverManager.getConnection("jdbc:sqlite:" + args[0]+""+name+".sqlite");
	    	        exporter.export(jdbc);
	        	} catch (Exception e){
	        		e.printStackTrace();
	        		System.out.println(e.getMessage());
	        	}
    		}
		}
    	
    	
    }
}
