package it.bitprepared.asa.exporter;


import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;

import org.junit.*;

/**
 * Test the {@link AccessExporter}
 * Vedi anche : https://bitbucket.org/xerial/sqlite-jdbc
 */
public class AccessExporterTests {

	
	private File festerno;
	
	@Before
    public void setUp () throws SQLException, ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
        festerno = new File("/tmp/prova.sqlite");
        sqlite = DriverManager.getConnection("jdbc:sqlite::memory:");
//        sqlite = DriverManager.getConnection("jdbc:sqlite:"+festerno.getAbsolutePath());
        URL url = AccessExporterTests.class.getClassLoader().getResource("test-database.mdb");
        URL url_emiro = AccessExporterTests.class.getClassLoader().getResource("estrazione_agesci.mdb");
        ACCESS_DB = new File(url.getPath());
        ACCESS_DB_EMIRO = new File(url_emiro.getPath());
    }
    
    @After
    public void tearDown () throws SQLException {
        sqlite.close();
//        festerno.deleteOnExit();
    }
    
    @Test
    public void testExport () throws IOException, SQLException {
    	Database db = DatabaseBuilder.open(ACCESS_DB);
        final AccessExporter exporter = new AccessExporter(db);
        exporter.export(sqlite);

	/* XXX: Test the results using JDBC */
    }
    
    @Test
    public void testExportEmiro () throws IOException, SQLException {
    	Database db = DatabaseBuilder.open(ACCESS_DB_EMIRO);
        final AccessExporter exporter = new AccessExporter(db);
        exporter.export(sqlite);
        DatabaseMetaData gmd = sqlite.getMetaData();
        ResultSet rs = gmd.getTables(null, null, "%", null);
        List<String> tabelle = new ArrayList<String>();
        while (rs.next()) {
        	tabelle.add(rs.getString(3));
        }
        Assert.assertEquals(4,tabelle.size());        
    }

    private Connection sqlite;
    
    /** An example Access database. Path is relative to the checkout -- hopefully this continues to work. */
    private static File ACCESS_DB;
    private static File ACCESS_DB_EMIRO;
}
