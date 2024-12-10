package controller;

import connection.ConfigLoader;
import connection.JDBCUtil;
import until.Email;

import java.sql.CallableStatement;
import java.sql.Connection;

public class SyncDateDimController {

    public static void main(String[] args) {
        // Tạo kết nối tới database Warehouse
        JDBCUtil jdbcUtil = new JDBCUtil(new ConfigLoader(System.getProperty("user.dir") + "\\src\\main\\resources\\config\\configDBWareHouse.properties"));
        Connection connection = jdbcUtil.getConnection();

        try {
            // Gọi stored procedure SyncDateDim
            CallableStatement callableStatement = connection.prepareCall("{CALL SyncDateDim()}");
            callableStatement.execute();
            System.out.println("Đồng bộ dữ liệu từ stagingdb.date_dim sang warehousedb.date_dim thành công!");
            
            // Gửi email thông báo thành công
            Email.sendEmail("21130514@st.hcmuaf.edu.vn", "Đồng bộ dữ liệu thành công!", "ETL Process Success");
        } catch (Exception e) {
            // Gửi email thông báo lỗi
            Email.sendEmail("21130514@st.hcmuaf.edu.vn", "Lỗi khi đồng bộ dữ liệu: " + e.getMessage(), "ETL Process Failed");
            e.printStackTrace();
        } finally {
            // Đóng kết nối
            JDBCUtil.closeConnection(connection);
        }
    }
}
