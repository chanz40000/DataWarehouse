package connection;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigLoader {
        private final Properties properties = new Properties();
        String addressConfig;


    public String getAddressConfig() {
        return addressConfig;
    }

    public void setAddressConfig(String addressConfig) {
        this.addressConfig = addressConfig;
    }

    //
    public ConfigLoader(String addressConfig) {
        this.addressConfig = addressConfig;
            try {
            InputStream inputStream = new FileInputStream(addressConfig);
            if(inputStream == null){
                throw new RuntimeException("Khong tim thay file config.properties");
            }

                properties.load(inputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        public  String getDb() {
            return properties.getProperty("db");
        }

        public String getHost() {
            return properties.getProperty("host");
        }

        public String getPort() {
            return properties.getProperty("port");
        }

        public String getNameDB() {
            return properties.getProperty("nameDB");
        }

        public String getUsername() {
            return properties.getProperty("username");
        }

        public String getPassword() {
            return properties.getProperty("password");
        }
}
