package controller;
//Kết nối database DW, không được thì gửi email thông báo lỗi
// được thì chuyển product_daily ở staging qua 
// Cập nhật trạng thái trong log là Success Transforming
// Đóng kết nối bên staging
//•	Trường hợp 1 là bên phía daily nó không thay đổi thì vẫn giữ nguyên cái cũ (sql)
//(dùng exists, not exsist, select * from …_staging_daily where not exist (…)), lưu tạm vào câu lệnh trên vào bảng nào đó là insert into table xxx và dùng câu lệnh trên.
//•	Trường hợp 2 là bên phía daily nó mới hoàn toàn so với dw thì insert vào trong dw (sql,)
//•	Trường hợp 3 là có một field nào đó thay đổi, thì mình tìm ra dòng đó nó ở đâu bên dw, cập nhật bên dw là expire và thêm dòng mới đó vào.  (sql)

import java.sql.CallableStatement;
import java.sql.Connection;

import connection.ConfigLoader;
import connection.JDBCUtil;
import until.Email;

public class LoadStagingToWarehouse {

    public static void main(String[] args) {
        // Tạo kết nối tới database
        JDBCUtil jdbcUtil = new JDBCUtil(new ConfigLoader(System.getProperty("user.dir") + "\\src\\main\\resources\\config\\configDBWareHouse.properties"));
        Connection connection = jdbcUtil.getConnection();

        try {
            // Kiểm tra trạng thái PS và gọi stored procedure TransformStagingToWarehouse
            CallableStatement callableStatement = connection.prepareCall("{CALL TransformStagingToWarehouse()}");
            callableStatement.execute();
            System.out.println("Chuyển dữ liệu từ Staging qua Warehouse thành công!");
            
            // Gửi email thông báo thành công
            Email.sendEmail("21130514@st.hcmuaf.edu.vn", "Chuyển dữ liệu thành công!", "ETL Process Success");
        } catch (Exception e) {
            // Gửi email thông báo lỗi
            Email.sendEmail("21130514@st.hcmuaf.edu.vn", "Lỗi khi chuyển dữ liệu: " + e.getMessage(), "ETL Process Failed");
            e.printStackTrace();
        } finally {
            JDBCUtil.closeConnection(connection);
        }
    }
}
