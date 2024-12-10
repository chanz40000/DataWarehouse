package controller;

import connection.ConfigLoader;
import connection.JDBCUtil;
import until.Email;
import java.sql.*;

public class DataMartLoader {
    private static JDBCUtil jdbcUtil;
    private ConfigLoader configLoader;
    private Connection connectionControl;
    private static Statement stmt = null;
    private static Connection conn = null;

    // 1. Kết nối tới cơ sở dữ liệu
    public void connectToDBControl() {
        try {
            // Khởi tạo ConfigLoader và JDBCUtil
            configLoader = new ConfigLoader(System.getProperty("user.dir") + "\\src\\main\\resources\\config\\configDBControl.properties");
            jdbcUtil = new JDBCUtil(configLoader);
            connectionControl = jdbcUtil.getConnection();

            if (connectionControl == null || connectionControl.isClosed()) {
                throw new SQLException("Kết nối cơ sở dữ liệu thất bại: Kết nối null hoặc đã đóng.");
            }

            System.out.println("Kết nối cơ sở dữ liệu thành công!");
        } catch (Exception e) {
            System.err.println("Lỗi kết nối cơ sở dữ liệu: " + e.getMessage());
            sendErrorEmail(e.getMessage()); // Gửi thông báo lỗi qua email
        }
    }

    // Gửi thông báo lỗi qua email
    private static void sendErrorEmail(String errorMessage) {
        try {
            Email email = new Email();
            String subject = "Thông báo lỗi kết nối cơ sở dữ liệu";
            String body = "Kính gửi đội ngũ,\n\nĐã xảy ra lỗi khi kết nối tới cơ sở dữ liệu:\n" +
                    errorMessage + "\n\nVui lòng kiểm tra hệ thống ngay lập tức.\n\nTrân trọng,\nHệ thống";
            email.sendEmail("21130438@st.hcmuaf.edu.vn", subject, body);
        } catch (Exception e) {
            System.err.println("Lỗi khi gửi email: " + e.getMessage());
        }
    }

    // Xuất dữ liệu từ Data Warehouse ra file CSV
    public static void exportDataToCSV() {
        String exportSQL = "SELECT BookID, Title, Authors, Publisher, PublishedDate, ISBN_10, Date, date_id " +
                "FROM warehousedb.book_Wh " +
                "INTO OUTFILE 'D:/dw/data/data_for_datamart.csv' " +
                "FIELDS TERMINATED BY ',' " +
                "ENCLOSED BY '\"' " +
                "LINES TERMINATED BY '\\n';";

        try {
            stmt.executeUpdate(exportSQL);
            System.out.println("Dữ liệu đã được xuất ra CSV thành công.");
        } catch (SQLException e) {
            System.err.println("Lỗi khi xuất dữ liệu: " + e.getMessage());
        }
    }

    // Tải dữ liệu từ CSV vào bảng tạm
    public static void loadDataFromCSV() {
        String loadDataSQL = "LOAD DATA INFILE 'D:/DW/Data/data_for_datamart.csv' " +
                "INTO TABLE datamartdb.temp_DataMart " +
                "FIELDS TERMINATED BY ',' " +
                "ENCLOSED BY '\"' " +
                "LINES TERMINATED BY '\\n' " +
                "(BookID, Title, Authors, Publisher, PublishedDate, ISBN_10, Date, date_id);";

        try {
            stmt.executeUpdate(loadDataSQL);
            System.out.println("Dữ liệu đã được tải vào bảng temp_DataMart thành công.");
        } catch (SQLException e) {
            System.err.println("Lỗi khi tải dữ liệu: " + e.getMessage());
        }
    }

    // Đổi tên bảng: DataMart thành temp_DataMart và temp_DataMart thành DataMart
    public static void renameTables() {
        String renameSQL = "RENAME TABLE datamartdb.DataMart TO temp_DataMart_old, " +
                "datamartdb.temp_DataMart TO DataMart;";

        try {
            stmt.executeUpdate(renameSQL);
            System.out.println("Đổi tên bảng thành công.");
        } catch (SQLException e) {
            System.err.println("Lỗi khi đổi tên bảng: " + e.getMessage());
        }
    }

    // Xóa bảng cũ nếu không còn cần thiết
    public static void dropOldTable() {
        String dropOldTableSQL = "DROP TABLE IF EXISTS datamartdb.temp_DataMart_old;";

        try {
            stmt.executeUpdate(dropOldTableSQL);
            System.out.println("Bảng tạm cũ đã bị xóa thành công.");
        } catch (SQLException e) {
            System.err.println("Lỗi khi xóa bảng cũ: " + e.getMessage());
        }
    }

    // Quy trình ETL: Xuất, Tải, Đổi tên, và Xóa bảng cũ
    public static void runETLProcess() {
        try (Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/controldb", "root", "password");
             Statement stmt = conn.createStatement()) {

            // Bước 1: Xuất dữ liệu từ Data Warehouse
            exportDataToCSV();

            // Bước 2: Tải dữ liệu vào bảng tạm
            loadDataFromCSV();

            // Bước 3: Đổi tên các bảng
            renameTables();

            // Bước 4: Xóa bảng cũ nếu không cần thiết
            dropOldTable();

            System.out.println("Quy trình ETL hoàn thành thành công!");
        } catch (SQLException e) {
            System.err.println("Lỗi trong quá trình ETL: " + e.getMessage());
            sendErrorEmail(e.getMessage());  // Gửi thông báo lỗi qua email
        }
    }

    // Phương thức main để chạy toàn bộ quy trình
    public static void main(String[] args) {
        DataMartLoader dataMartLoader = new DataMartLoader();
        dataMartLoader.connectToDBControl();  // Kết nối đến cơ sở dữ liệu
        runETLProcess();  // Chạy quy trình ETL
    }
}
