import connection.ConfigLoader;
import connection.JDBCUtil;
import controller.LoadDataToStagingDirect;

import java.sql.Connection;

public class Test {
    public static void main(String[] args) {
        ConfigLoader configLoader = new ConfigLoader(System.getProperty("user.dir") + "\\src\\main\\resources\\config\\configStaging.properties");
        Connection connection = null;

        JDBCUtil jdbcUtil = new JDBCUtil(configLoader);
        try {
            // 2. Ket noi database staging
            connection = jdbcUtil.getConnection();

            String correctedFilePath = "C:\\Users\\ADMIN\\Downloads\\DW\\Data\\books_data_30-10-2024.csv";
            //  String correctedFilePath = "C:\\Users\\ADMIN\\Downloads\\DW\\Data\\date_dim_without_quarter.csv";

//            LoadDataToStagingDirect.loadDataToBookDaily(correctedFilePath);
            // In thông tin kết nối
            JDBCUtil.printInfo(connection);

            // Đóng kết nối
            JDBCUtil.closeConnection(connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
