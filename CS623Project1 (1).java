
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CS623Project1 {

    private Connection conn = null;

    public CS623Project1() {
        this.setConnection();
    }

    public Connection getConn() {
        return conn;
    }

    public void setConnection() {
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:8008/cs623", "postgres", "Richitha@123");

            this.getConn().setAutoCommit(false);

            // For Isolation
            this.getConn().setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createTables() {
        try {
            createProductTable();
            createDepotTable();
            createStockTable();
            conn.commit();
            System.out.println("Tables are Created ");
        } catch (SQLException e) {
            handleSQLException(e);
        }
    }

    private void createProductTable() throws SQLException {
        Statement stmt = conn.createStatement();
        String createTableProd = "CREATE TABLE Product(prod_id CHAR(10), pname VARCHAR(30), price decimal)";
        stmt.executeUpdate(createTableProd);
        stmt.close();
    }

    private void createDepotTable() throws SQLException {
        Statement stmt = conn.createStatement();
        String createTableDepot = "CREATE TABLE Depot(Dep_id CHAR(20), address VARCHAR(25), volume INTEGER)";
        stmt.executeUpdate(createTableDepot);
        stmt.close();
    }

    private void createStockTable() throws SQLException {
        Statement stmt = conn.createStatement();
        String createTableStock = "CREATE TABLE Stock(prod_id CHAR(20), Dep_id CHAR(20), quantity INTEGER)";
        stmt.executeUpdate(createTableStock);
        stmt.close();
    }

    public void insertValues() {
        try {
            insertProductValues();
            insertDepotValues();
            insertStockValues();
            conn.commit();
        } catch (SQLException e) {
            handleSQLException(e);
        }
    }

    private void insertProductValues() throws SQLException {
        Statement query = conn.createStatement();
        query.execute("INSERT INTO product VALUES('P1', 'tape', '2.5')");
        query.execute("INSERT INTO product VALUES('P2', 'TV', '250')");
        query.execute("INSERT INTO product VALUES('P3', 'VCR', '80')");
        query.execute("INSERT INTO product VALUES('P100', 'CD', '5')");
        query.close();
    }

    private void insertDepotValues() throws SQLException {
        Statement query = conn.createStatement();
        query.execute("INSERT INTO depot VALUES('D1', 'New York', 9000)");
        query.execute("INSERT INTO depot VALUES('D2', 'Syracuse', 6000)");
        query.execute("INSERT INTO depot VALUES('D3', 'New York', 2000)");
        query.close();
    }

    private void insertStockValues() throws SQLException {
        Statement query = conn.createStatement();
        query.execute("INSERT INTO stock VALUES('P1', 'D2', 1000)");
        query.execute("INSERT INTO stock VALUES('P1', 'D3', 1200)");
        query.execute("INSERT INTO stock VALUES('P3', 'D1', 3000)");
        query.execute("INSERT INTO stock VALUES('P3', 'D3', 2000)");
        query.execute("INSERT INTO stock VALUES('P2', 'D3', 1500)");
        query.execute("INSERT INTO stock VALUES('P2', 'D1', -400)");
        query.execute("INSERT INTO stock VALUES('P2', 'D2', 2000)");
        query.execute("INSERT INTO stock VALUES('P100', 'D2', 50)");
        query.close();
    }

    public void addConstraints() {
        try {
            addProductConstraints();
            addDepotConstraints();
            addStockConstraints();
            conn.commit();
            System.out.println("Constraints added");
        } catch (SQLException e) {
            handleSQLException(e);
        }
    }

    private void addProductConstraints() throws SQLException {
        Statement query = conn.createStatement();
        query.execute("ALTER TABLE Product ADD CONSTRAINT pk_product PRIMARY KEY(prod_id)");
        query.close();
    }

    private void addDepotConstraints() throws SQLException {
        Statement query = conn.createStatement();
        query.execute("ALTER TABLE depot ADD CONSTRAINT pk_depot PRIMARY KEY(dep_id)");
        query.close();
    }

    private void addStockConstraints() throws SQLException {
        Statement query = conn.createStatement();
        query.execute("ALTER TABLE stock ADD CONSTRAINT pk_stock PRIMARY KEY(prod_id, dep_id)");
        query.execute("ALTER TABLE stock ADD CONSTRAINT fk_stock_product FOREIGN KEY(prod_id) REFERENCES product(prod_id) ON DELETE CASCADE");
        query.execute("ALTER TABLE stock ADD CONSTRAINT fk_stock_depot FOREIGN KEY(dep_id) REFERENCES depot(dep_id) ON DELETE CASCADE");
        query.close();
    }

    private void handleSQLException(SQLException e) {
        System.out.println("An exception was thrown");
        e.printStackTrace();
        try {
            conn.rollback();
            conn.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String args[]) {
        CS623Project1 project_demo = new CS623Project1();

        project_demo.createTables();
        project_demo.insertValues();
        project_demo.addConstraints();
    }
}
