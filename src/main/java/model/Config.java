package model;

import java.sql.Timestamp;

public class Config {
    private int idConfig;
    private String configName;
    private String address;
    private String description;
    private String createBy;
    private Timestamp createAt;
    private String updateBy;
    private Timestamp updateAt;

    // Constructor không tham số
    public Config() {}

    // Constructor đầy đủ tham số
    public Config(int idConfig, String configName, String address, String description, String createBy,
                  Timestamp createAt, String updateBy, Timestamp updateAt) {
        this.idConfig = idConfig;
        this.configName = configName;
        this.address = address;
        this.description = description;
        this.createBy = createBy;
        this.createAt = createAt;
        this.updateBy = updateBy;
        this.updateAt = updateAt;
    }

    // Getter và Setter cho từng thuộc tính
    public int getIdConfig() {
        return idConfig;
    }

    public void setIdConfig(int idConfig) {
        this.idConfig = idConfig;
    }

    public String getConfigName() {
        return configName;
    }

    public void setConfigName(String configName) {
        this.configName = configName;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCreateBy() {
        return createBy;
    }

    public void setCreateBy(String createBy) {
        this.createBy = createBy;
    }

    public Timestamp getCreateAt() {
        return createAt;
    }

    public void setCreateAt(Timestamp createAt) {
        this.createAt = createAt;
    }

    public String getUpdateBy() {
        return updateBy;
    }

    public void setUpdateBy(String updateBy) {
        this.updateBy = updateBy;
    }

    public Timestamp getUpdateAt() {
        return updateAt;
    }

    public void setUpdateAt(Timestamp updateAt) {
        this.updateAt = updateAt;
    }

    // Override phương thức toString() để hiển thị thông tin của Config
    @Override
    public String toString() {
        return "Config{" +
                "idConfig=" + idConfig +
                ", configName='" + configName + '\'' +
                ", address='" + address + '\'' +
                ", description='" + description + '\'' +
                ", createBy='" + createBy + '\'' +
                ", createAt=" + createAt +
                ", updateBy='" + updateBy + '\'' +
                ", updateAt=" + updateAt +
                '}';
    }
}
