package controller;

import connection.ConfigLoader;
import connection.JDBCUtil;
import model.Book;
import until.Email;
import com.google.gson.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BookDataFetcher {

    private static final String API_KEY = "AIzaSyBj3eikulnwGQayKVmZBENgCyaZMlIBN9g"; // Cập nhật API Key của bạn
    private static JDBCUtil jdbcUtil;
    ConfigLoader configLoader;
    Connection connectionControll;

    //1. ket noi db control
    public void connectToDBConTrol() {
        try {
            // Khởi tạo configLoader và jdbcUtil
            configLoader = new ConfigLoader(System.getProperty("user.dir") + "\\src\\main\\resources\\config\\configDBControll.properties");
            jdbcUtil = new JDBCUtil(configLoader);
            connectionControll = jdbcUtil.getConnection();

            if (connectionControll == null || connectionControll.isClosed()) {
                throw new SQLException("Không thể kết nối đến cơ sở dữ liệu. Connection is null or closed.");
            }

            System.out.println("Kết nối cơ sở dữ liệu thành công!");
        } catch (Exception e) {
            System.out.println("Lỗi kết nối cơ sở dữ liệu: " + e.getMessage());

            // Gửi email thông báo lỗi
            sendErrorEmail(e.getMessage());
        }
    }

    // Gửi email thông báo lỗi
    private void sendErrorEmail(String errorMessage) {
        try {
            // Tạo đối tượng Email
            Email email = new Email();

            // Cấu hình thông tin email
            String subject = "Thông báo lỗi kết nối cơ sở dữ liệu";
            String body = "Kính gửi,\n\n" +
                    "Có lỗi xảy ra khi kết nối cơ sở dữ liệu:\n" +
                    errorMessage + "\n\n" +
                    "Vui lòng kiểm tra hệ thống ngay lập tức.\n\n" +
                    "Trân trọng,\nHệ thống";
            email.sendEmail("21130438@st.hcmuaf.edu.vn", subject, body); // Thay địa chỉ email nhận thật
            System.out.println("Đã gửi email thông báo lỗi thành công.");
        } catch (Exception e) {
            System.out.println("Lỗi khi gửi email thông báo: " + e.getMessage());
        }
    }


    // Gọi API Google Books và lấy dữ liệu sách
    public static String fetchBooksFromGoogle(String query) {
        try {
            String urlString = "https://www.googleapis.com/books/v1/volumes?q=" + query + "&key=" + API_KEY;
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            return response.toString();
        } catch (Exception e) {
            System.out.println("Lỗi khi gọi Google Books API: " + e.getMessage());
            return "";
        }
    }

    // Gọi API Open Library và lấy dữ liệu sách
    public static String fetchBooksFromOpenLibrary(String query) {
        try {
            String urlString = "https://openlibrary.org/search.json?q=" + query;
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            return response.toString();
        } catch (Exception e) {
            System.out.println("Lỗi khi gọi Open Library API: " + e.getMessage());
            return "";
        }
    }

    private static Set<String> processedConfigs = new HashSet<>();

    public static void writeBooksToCSV(List<Book> books, String filePath, String configName) {
        // Kiểm tra nếu cấu hình đã được xử lý
        if (processedConfigs.contains(configName)) {
            System.out.println("Cấu hình " + configName + " đã được xử lý trước đó, bỏ qua...");
            return;  // Dừng xử lý nếu cấu hình đã được xử lý
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, StandardCharsets.UTF_8))) {
            // Ghi tiêu đề cột
            writer.write("BookID;Title;Authors;Publisher;PublishedDate;ISBN_10;Date");
            writer.newLine();

            // Ghi từng dòng dữ liệu
            for (Book book : books) {
                String line = String.format("%s;%s;%s;%s;%s;%s;%s",
                        book.getId(),
                        book.getTitle(),
                        book.getAuthors(),
                        book.getPublisher(),
                        book.getPublishedDate(),
                        book.getIsbn(),
                        book.getDate());
                writer.write(line);
                writer.newLine();
            }
            System.out.println("Ghi file CSV thành công!");

            // Thêm cấu hình vào danh sách đã xử lý
            processedConfigs.add(configName);
            System.out.println("Danh sách cấu hình đã xử lý: " + processedConfigs);

        } catch (IOException e) {
            System.out.println("Lỗi khi ghi file CSV: " + e.getMessage());
        }
    }

    // Trích xuất dữ liệu từ Google Books API và chuyển thành danh sách Book
    public static List<Book> extractGoogleBooksData(String jsonResponse) {
        List<Book> books = new ArrayList<>();
        try {
            JsonObject jsonObject = JsonParser.parseString(jsonResponse).getAsJsonObject();
            if (jsonObject.has("items")) {
                JsonArray items = jsonObject.getAsJsonArray("items");

                for (JsonElement item : items) {
                    JsonObject book = item.getAsJsonObject().getAsJsonObject("volumeInfo");

                    // Trích xuất thông tin sách
                    String id = item.getAsJsonObject().get("id").getAsString();
                    String title = book.has("title") ? book.get("title").getAsString() : null;

                    // Kiểm tra xem tiêu đề có tồn tại không
                    if (title == null) continue;

                    // Xử lý authors: lấy 3 tác giả đầu tiên
                    String authors = "Không có thông tin tác giả";
                    if (book.has("authors")) {
                        JsonArray authorsArray = book.getAsJsonArray("authors");
                        List<String> authorList = new ArrayList<>();
                        for (int i = 0; i < Math.min(3, authorsArray.size()); i++) {
                            authorList.add(authorsArray.get(i).getAsString());
                        }
                        authors = String.join(", ", authorList);
                    }

                    // Xử lý publisher: lấy nhà xuất bản
                    String publisher = book.has("publisher") ? book.get("publisher").getAsString() : null;
                    if (publisher == null) continue; // Bỏ qua nếu không có nhà xuất bản

                    String publishedDate = book.has("publishedDate") ? book.get("publishedDate").getAsString() : null;
                    if (publishedDate == null) continue; // Bỏ qua nếu không có ngày xuất bản

                    // Trích xuất ISBN-10 từ industryIdentifiers
                    String isbn10 = "N/A"; // Mặc định là "N/A"
                    JsonArray industryIdentifiers = book.has("industryIdentifiers") ? book.getAsJsonArray("industryIdentifiers") : new JsonArray();
                    for (JsonElement identifier : industryIdentifiers) {
                        JsonObject identifierObj = identifier.getAsJsonObject();
                        if ("ISBN_10".equals(identifierObj.get("type").getAsString())) {
                            isbn10 = identifierObj.get("identifier").getAsString();
                            break;
                        }
                    }

                    // Kiểm tra nếu tất cả dữ liệu quan trọng đều có giá trị
                    String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
                    if (id != null && title != null && publisher != null && publishedDate != null) {
                        Book bookObj = new Book(id, title, authors, publisher, publishedDate, isbn10, currentDate);
                        books.add(bookObj);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return books;
    }

    // Trích xuất dữ liệu từ Open Library API và chuyển thành danh sách Book
    public static List<Book> extractOpenLibraryData(String jsonResponse) {
        List<Book> books = new ArrayList<>();
        try {
            JsonObject jsonObject = JsonParser.parseString(jsonResponse).getAsJsonObject();
            if (jsonObject.has("docs")) {
                JsonArray docs = jsonObject.getAsJsonArray("docs");

                for (JsonElement doc : docs) {
                    JsonObject book = doc.getAsJsonObject();

                    // Trích xuất thông tin sách
                    String title = book.has("title") ? book.get("title").getAsString() : null;
                    if (title == null) continue;

                    String authors = "Không có thông tin tác giả";
                    if (book.has("author_name")) {
                        JsonArray authorArray = book.getAsJsonArray("author_name");
                        List<String> authorList = new ArrayList<>();
                        for (int i = 0; i < Math.min(3, authorArray.size()); i++) {
                            authorList.add(authorArray.get(i).getAsString());
                        }
                        authors = String.join(", ", authorList);
                    }

                    String publisher = book.has("publisher") ? book.getAsJsonArray("publisher").get(0).getAsString() : null;
                    if (publisher == null) continue;

                    String isbn10 = "N/A";
                    if (book.has("isbn")) {
                        JsonArray isbnArray = book.getAsJsonArray("isbn");
                        for (JsonElement isbn : isbnArray) {
                            if (isbn.getAsString().length() == 10) {
                                isbn10 = isbn.getAsString();
                                break;
                            }
                        }
                    }

                    String publishedDate = "N/A";  // Open Library không luôn có ngày xuất bản
                    if (book.has("first_publish_year")) {
                        publishedDate = book.get("first_publish_year").getAsString();
                    }

                    // Kiểm tra xem các trường dữ liệu quan trọng có đầy đủ không
                    String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
                    if (title != null && publisher != null && isbn10 != null && publishedDate != null) {
                        Book bookObj = new Book("openlibrary-" + book.get("key").getAsString(), title, authors, publisher, publishedDate, isbn10,currentDate);
                        books.add(bookObj);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return books;
    }

    public static void processBooks(String query) {
        Set<String> processedConfigs = new HashSet<>();  // Sử dụng Set để lưu cấu hình đã xử lý

        try {
            // Chèn cấu hình vào bảng config nếu chưa có dữ liệu, bao gồm ngày hiện tại trong đường dẫn
            String insertConfigQuery = "INSERT INTO config (config_name, address, descriptionn, create_by, create_at, update_by, update_at) " +
                    "VALUES (?, CONCAT(?, '_books_', DATE_FORMAT(CURDATE(), '%Y-%m-%d'), '.csv'), ?, ?, NOW(), NULL, NULL), (?, CONCAT(?, '_books_', DATE_FORMAT(CURDATE(), '%Y-%m-%d'), '.csv'), ?, ?, NOW(), NULL, NULL)";

            try (Connection conn = jdbcUtil.getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(insertConfigQuery)) {

                // Chèn cấu hình Google Books
                pstmt.setString(1, "googleBook");
                pstmt.setString(2, "D:/dw/data/googleBook/googleBook"); // Thêm googleBook vào để tạo tên folder
                pstmt.setString(3, "data for google book");
                pstmt.setString(4, "admin");

                // Chèn cấu hình Open Library
                pstmt.setString(5, "openLibrary");
                pstmt.setString(6, "D:/dw/data/openLibrary/openLibrary"); // Thêm openLibrary vào để tạo tên folder
                pstmt.setString(7, "data for openLibrary");
                pstmt.setString(8, "admin");

                // Thực thi câu lệnh
                int rowsInserted = pstmt.executeUpdate();
                System.out.println(rowsInserted + " cấu hình đã được chèn vào bảng config.");
            } catch (SQLException e) {
                System.out.println("Lỗi khi chèn cấu hình vào bảng config: " + e.getMessage());
            }

            // Truy vấn thông tin cấu hình từ cơ sở dữ liệu
            String queryConfig = "SELECT id_config, config_name, address FROM config WHERE config_name IN ('googleBook', 'openLibrary')";
            try (Connection conn = jdbcUtil.getConnection(); Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(queryConfig)) {
                while (rs.next()) {
                    int idConfig = rs.getInt("id_config");
                    String configName = rs.getString("config_name");
                    String address = rs.getString("address");

                    System.out.println("Đang kiểm tra cấu hình: " + configName);

                    // Kiểm tra dữ liệu có phải là mới nhất không
                    if (!isConfigCreatedToday(idConfig)) {
                        System.out.println("Cấu hình " + configName + " không phải dữ liệu mới nhất.");

                        // Gửi email thông báo lỗi
                        String errorMessage = "Dữ liệu cho cấu hình " + configName + " không phải mới nhất.";
                        String subject = "Thông báo lỗi: Dữ liệu không mới";
                        Email.sendEmail("21130438@st.hcmuaf.edu.vn", errorMessage, subject);

                        // Ghi log với trạng thái "Failed"
                        logStatus(idConfig, "Failed", "Dữ liệu không phải mới nhất.");
                        continue; // Bỏ qua cấu hình này
                    }

                    // Lấy dữ liệu sách từ các API
                    List<Book> books = new ArrayList<>();
                    if ("googleBook".equals(configName)) {
                        String response = fetchBooksFromGoogle(query);
                        books = extractGoogleBooksData(response);
                    } else if ("openLibrary".equals(configName)) {
                        String response = fetchBooksFromOpenLibrary(query);
                        books = extractOpenLibraryData(response);
                    }

                    // Lưu dữ liệu sách vào CSV
                    String filePath = address;
                    writeBooksToCSV(books, filePath, configName);

                    // Ghi log với trạng thái thành công
                    logStatus(idConfig, "RE", "Dữ liệu đã được lưu vào file CSV.");
                }
            } catch (SQLException e) {
                System.out.println("Lỗi khi xử lý dữ liệu: " + e.getMessage());

                // Gửi email thông báo lỗi
                String errorMessage = "Lỗi SQL: " + e.getMessage();
                String subject = "Thông báo lỗi trong quá trình xử lý dữ liệu";
                Email.sendEmail("21130438@st.hcmuaf.edu.vn", errorMessage, subject);

                // Ghi vào log với trạng thái "Failed"
                logStatus(0, "Failed", "Lỗi trong quá trình xử lý.");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    public static boolean isConfigCreatedToday(int idConfig) {
        String query = "SELECT create_at FROM config WHERE id_config = ?";
        try (Connection conn = jdbcUtil.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setInt(1, idConfig);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    Timestamp createAt = rs.getTimestamp("create_at");
                    LocalDate createDate = createAt.toLocalDateTime().toLocalDate();
                    return createDate.isEqual(LocalDate.now());
                }
            }
        } catch (SQLException e) {
            System.out.println("Lỗi khi kiểm tra ngày create_at: " + e.getMessage());
        }
        return false;
    }

    // Ghi log vào bảng log
    public static void logStatus(int idConfig, String status, String detail) {
        String query = "INSERT INTO log (id_config, status_config, time_RE, time_PS, detail) VALUES (?, ?, NOW(), NULL, ?)";
        try (Connection conn = jdbcUtil.getConnection(); PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setInt(1, idConfig);
            stmt.setString(2, status);
            stmt.setString(3, detail);
            stmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Lỗi khi ghi vào bảng log: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        // Khởi tạo kết nối cơ sở dữ liệu
        BookDataFetcher bookDataFetcher = new BookDataFetcher();
        bookDataFetcher.connectToDBConTrol();

        String query = "IT";  // Từ khóa tìm kiếm sách
        bookDataFetcher.processBooks(query);
    }
}
