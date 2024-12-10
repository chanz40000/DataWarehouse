package controller;

import connection.ConfigLoader;
import connection.JDBCUtil;
import model.Config;
import until.Email;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class LoadDataToStagingDirect {
    ConfigLoader configLoader;
    JDBCUtil jdbcUtil;
    Connection connectionControll;
    //Connection connectionStaging;

    //
    //1. ket noi db control
    public void connectToDBConTrol(){
        configLoader = new ConfigLoader(System.getProperty("user.dir") + "\\src\\main\\resources\\config\\configDBControll.properties");
        jdbcUtil = new JDBCUtil(configLoader);
        connectionControll = jdbcUtil.getConnection();
    }


    //2. check log lay ra list id_config co status la RE
    public List<Integer> callCheckLog() {
        // Tạo danh sách để lưu id_config
        List<Integer> idConfigList = new ArrayList<>();
        String procedureCall = "{CALL CheckLog()}";

        try (CallableStatement callableStatement = connectionControll.prepareCall(procedureCall);
             ResultSet resultSet = callableStatement.executeQuery()) {

            // Lặp qua kết quả trả về và thêm vào danh sách
            while (resultSet.next()) {
                int idConfig = resultSet.getInt("id_config");
                idConfigList.add(idConfig);
            }

            // In danh sách id_config
            System.out.println("Danh sách id_config có status_config = 'RE': " + idConfigList);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return idConfigList;
    }

    //phuong thuc lay ra config tu id
    public  Config getConfigById(int id, Connection connection) {
        // Khai báo đối tượng Config để lưu dữ liệu trả về
        Config config = null;
        String procedureCall = "{CALL GetConfigById(?)}";

        try (CallableStatement callableStatement = connection.prepareCall(procedureCall)) {
            // Truyền tham số cho procedure
            callableStatement.setInt(1, id);

            // Thực thi procedure và nhận kết quả
            try (ResultSet resultSet = callableStatement.executeQuery()) {
                if (resultSet.next()) {
                    // Lấy dữ liệu từ ResultSet
                    int idConfig = resultSet.getInt("id_config");
                    String configName = resultSet.getString("config_name");
                    String address = resultSet.getString("address");
                    String description = resultSet.getString("descriptionn");
                    String createBy = resultSet.getString("create_by");
                    Timestamp createAt = resultSet.getTimestamp("create_at");
                    String updateBy = resultSet.getString("update_by");
                    Timestamp updateAt = resultSet.getTimestamp("update_at");

                    // Tạo đối tượng Config
                    config = new Config(idConfig, configName, address, description, createBy, createAt, updateBy, updateAt);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return config;

    }

    //6. Ghi log doi status cua config trong log thanh PS va cap nhat thoi gian time_ps
    public static boolean callSetStatusToPS(int idConfig, Connection connection) {
        String procedureCall = "{CALL SetStatusToPS(?)}";

        try (CallableStatement callableStatement = connection.prepareCall(procedureCall)) {
            // Truyền tham số `idConfig` cho procedure
            callableStatement.setInt(1, idConfig);

            // Thực thi procedure
            callableStatement.execute();
            System.out.println("Cập nhật trạng thái PS thành công cho id_config: " + idConfig);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Cập nhật trạng thái PS thất bại cho id_config: " + idConfig);
            return false;
        }
    }



    //4. Load data vao table book_daily trong db staging
    public static int loadDataToBookDailyTemp(Connection connection, String filePath) throws SQLException, SQLException {
        String correctedFilePath = filePath.replace("\\", "\\\\");

        String truncateSql = "TRUNCATE TABLE stagingdb.book_daily_temp";


        // LOAD DATA INFILE
        String loadSql = "LOAD DATA LOCAL  INFILE '" + correctedFilePath + "' " +
                "INTO TABLE stagingdb.book_daily_temp " +
                "FIELDS TERMINATED BY ';' " +
                "LINES TERMINATED BY '\\n' " +
                "(Field1,\n" +
                "    Field2,\n" +
                "    Field3,\n" +
                "    Field4,\n" +
                "    Field5,\n" +
                "    Field6,\n" +
                "    Field7);";

        try (Statement stmt = connection.createStatement()) {

            stmt.execute(truncateSql);  // Xóa dữ liệu cũ trong bảng staging


            int rowCount = stmt.executeUpdate(loadSql);  // Thực thi LOAD DATA INFILE

            return rowCount;
        }catch (Exception e){
            //4.1 neu that bai thi gui mail bao that bai
            System.out.println(e);
            Email.sendEmail("21130574@st.hcmuaf.edu.vn", "Load data vao Staging file: "+filePath+ " THAT BAI", "Thong bao WH");
        }
        return 0;
    }
    //5. load data to table book-daily
    public static boolean loadDataToBookDaily(Connection connection) {
        String procedureCall = "{CALL stagingdb.LoadDataToBookDaily()}";

        try (CallableStatement callableStatement = connection.prepareCall(procedureCall)) {
            // Thực thi procedure
            callableStatement.execute();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Cập nhật trạng thái PS thất bại cho id_config: " + e.getMessage());
            return false;
        }
    }
    public static int callLoadDateDimProcedure(Connection connection, String filePath) {
        try {
            // Chuẩn hóa đường dẫn tệp (nếu chạy trên Windows)
            String correctedFilePath = filePath.replace("\\", "/");

            // Gọi procedure LoadDateDim
            CallableStatement callableStatement = connection.prepareCall("{CALL LoadDateDim(?)}");
            callableStatement.setString(1, correctedFilePath);

            // Thực thi procedure
            callableStatement.execute();
            System.out.println("Load data vào date_dim thành công!");
            return 1; // Success
        } catch (Exception e) {
            e.printStackTrace();
            // Gửi email thông báo lỗi nếu cần
            Email.sendEmail("21130574@st.hcmuaf.edu.vn", "Load data vào date_dim file: " + filePath + " THẤT BẠI"+ e.getMessage(), "Thông báo WH");
            return 0; // Failure
        }
    }


    public void runLoadDataToStaging() throws SQLException {
        //1. ket noi db controll
        connectToDBConTrol();

        //2. check log lay ra list id_config co status la RE
        List<Integer> idConfigList = callCheckLog();

        //kiem tra neu list.size khac 0
        if(idConfigList.size()>0){

            String senEmail = "";

            Config config;
            //3. duyet tung config_id trong list
            for (int i = 0; i<idConfigList.size(); i++){
                config = getConfigById(idConfigList.get(i), connectionControll);

                //4. Load data vao table book_daily_temp
                loadDataToBookDailyTemp(connectionControll, config.getAddress());
                //5. load data to table book-daily

                boolean success=loadDataToBookDaily(connectionControll);
                //6. Cập nhật bảng log
                if(success){
                    callSetStatusToPS(idConfigList.get(i), connectionControll);

                    senEmail+="\n"+"Load data vao Staging thanh cong, file csv duoc tao boi: "+config.getCreateBy()
                            + ", ngay tao: "+ config.getCreateAt().toString()+"\n";
                }else{
                    senEmail+="\n"+"Load data vao Staging that bai khi load vao table book_daily, file csv duoc tao boi: "+config.getCreateBy()
                            + ", ngay tao: "+ config.getCreateAt().toString()+"\n";
                }

            }
            //7. gui mail bao ket thuc
            Email.sendEmail("21130574@st.hcmuaf.edu.vn", senEmail, "Thong bao WH");
        }else{
            //2.1 gui mail bao  khong co data nhap vao
            Email.sendEmail("21130574@st.hcmuaf.edu.vn", "Load data vao Staging ngay: "+ LocalDate.now()+", khong co data nhap vao", "Thong bao WH");
        }


    }


    public static void main(String[] args) throws SQLException {
        LoadDataToStagingDirect loadDataToStagingDirect = new LoadDataToStagingDirect();
        loadDataToStagingDirect.runLoadDataToStaging();
    }


}
