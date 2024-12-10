package controller;

import connection.ConfigLoader;
import connection.JDBCUtil;
import until.Email;

import java.sql.CallableStatement;
import java.sql.Connection;

public class UpdateDateIdWarehouse {

    public static void main(String[] args) {
        // Tạo kết nối tới database Warehouse
        JDBCUtil jdbcUtil = new JDBCUtil(new ConfigLoader(System.getProperty("user.dir") + "\\src\\main\\resources\\config\\configDBWareHouse.properties"));
        Connection connection = jdbcUtil.getConnection();

        try {
            // Gọi stored procedure UpdateDateIdForTables
            CallableStatement callableStatement = connection.prepareCall("{CALL UpdateDateIdWarehouse()}");
            callableStatement.execute();
            System.out.println("Cập nhật date_id cho temp_book_update và book_Wh thành công!");
            
            // Gửi email thông báo thành công
            Email.sendEmail("21130514@st.hcmuaf.edu.vn", "Cập nhật date_id thành công!", "ETL Process Success");
        } catch (Exception e) {
            // Gửi email thông báo lỗi
            Email.sendEmail("21130514@st.hcmuaf.edu.vn", "Lỗi khi cập nhật date_id: " + e.getMessage(), "ETL Process Failed");
            e.printStackTrace();
        } finally {
            // Đóng kết nối
            JDBCUtil.closeConnection(connection);
        }
    }
}
