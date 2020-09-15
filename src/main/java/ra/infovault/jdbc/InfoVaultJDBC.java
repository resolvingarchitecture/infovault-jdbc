package ra.infovault.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public abstract class InfoVaultJDBC implements ra.common.InfoVaultDB {

    public static final String DB = "ra.infovault.jdbc.db";
    public static final String DB_URL = "ra.infovault.jdbc.url";

    public static String location;

    private static final Logger LOGGER = Logger.getLogger(InfoVaultJDBC.class.getName());

    private Properties config;
    private String databaseURL;

    public Connection getConnection() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(databaseURL);
        } catch (SQLException e) {
            LOGGER.warning(e.getLocalizedMessage());
        }
        return conn;
    }

    public boolean doesTableExists(String tableName, Connection conn) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet result = meta.getTables(null, null, tableName.toUpperCase(), null);

        return result.next();
    }

    @Override
    public boolean start(Properties properties) {
        config = properties;
        if(location==null) {
            LOGGER.severe("Location as absolute path, including database name, is required to be set on InfoVaultDerby.");
            return false;
        }
        databaseURL = config.getProperty(DB_URL);
        try {
            Connection conn = DriverManager.getConnection(databaseURL);
        } catch (SQLException e) {
            LOGGER.severe(e.getLocalizedMessage());
            return false;
        }
        return true;
    }

    @Override
    public boolean pause() {
        return false;
    }

    @Override
    public boolean unpause() {
        return false;
    }

    @Override
    public boolean restart() {
        return false;
    }

    @Override
    public boolean shutdown() {
        return true;
    }

    @Override
    public boolean gracefulShutdown() {
        return shutdown();
    }
}
