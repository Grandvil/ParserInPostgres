import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DB {
    //public Connection connection;

    public Connection connect() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/counters","root","root");
            if (connection != null) {
                System.out.println("Connected to the database!");
            } else {
                System.out.println("Failed to make connection!");
            }
           // return connection;
        } catch(SQLException e){
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        } catch(Exception e){
            e.printStackTrace();
        }
        return connection;
    }
}
