package it.bitprepared.asa.exporter;


import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;

import org.junit.*;

/**
 * Test the {@link AccessExporter}
 */
public class AccessExporterTests {
    @Before
    public void setUp () throws SQLException, ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
        sqlite = DriverManager.getConnection("jdbc:sqlite::memory:");
        URL url = AccessExporterTests.class.getClassLoader().getResource("test-database.mdb");
        ACCESS_DB = new File(url.getPath());
    }
    
    @After
    public void tearDown () throws SQLException {
        sqlite.close();
    }
    
    @Test
    public void testExport () throws IOException, SQLException {
    	Database db = DatabaseBuilder.open(ACCESS_DB);
        final AccessExporter exporter = new AccessExporter(db);
        exporter.export(sqlite);

	/* XXX: Test the results using JDBC */
    }

    private Connection sqlite;
    
    /** An example Access database. Path is relative to the checkout -- hopefully this continues to work. */
    private static File ACCESS_DB;
}
