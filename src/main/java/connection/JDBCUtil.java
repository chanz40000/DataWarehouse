package connection;


import controller.LoadDataToStagingDirect;
import until.Email;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCUtil {

    ConfigLoader configLoader;

    public JDBCUtil(ConfigLoader configLoader) {
        this.configLoader = configLoader;
    }

    public Connection getConnection() {
        Connection c = null;

        try {

            // 1.Kết nối đến database
            String urlDb = "jdbc:" + configLoader.getDb() + "://" + configLoader.getHost() + ":" + configLoader.getPort() + "/" + configLoader.getNameDB()+"?allowLoadLocalInfile=true";
                Class.forName("com.mysql.cj.jdbc.Driver");

            c = DriverManager.getConnection(urlDb, configLoader.getUsername(), configLoader.getPassword());

            //1.1---3.1 gui thong bao ket noi thanh cong
            //Email.sendEmail("21130574@st.hcmuaf.edu.vn", "Ket noi stagingdb thanh cong!!!", "Thong bao ket qua ket noi db");

        } catch (SQLException | ClassNotFoundException e) {
            //1.1 gui thong bao ket noi that bai
            Email.sendEmail("21130574@st.hcmuaf.edu.vn", "Ket noi stagingdb bi that bai, loi do: "+ e.getMessage(), "Thong bao ket qua ket noi db");
            throw new RuntimeException(e);
        }
        return c;
    }
    public static void closeConnection(Connection c) {
        try {
            if(c!=null) {
                c.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public static void printInfo(Connection c) {
        try {
            if(c!=null) {
                DatabaseMetaData mtdt = c.getMetaData();
                System.out.println(mtdt.getDatabaseProductName());
                System.out.println(mtdt.getDatabaseProductVersion());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
