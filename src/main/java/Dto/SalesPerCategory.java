package Dto;
import java.io.Serializable;
import java.sql.Date;
public class SalesPerCategory implements Serializable {
    private Date transactionDate;
    private String category;
    private Double totalSales;
    public SalesPerCategory(){
        this.transactionDate= null;
        this.category = "";
        this.totalSales = 0.0;
    }
    public SalesPerCategory(Date transactionDate, String category, Double totalSales) {
        this.transactionDate = transactionDate;
        this.category = category;
        this.totalSales = totalSales;
    }

    public Date getTransactionDate() {
        return transactionDate;
    }

    public String getCategory() {
        return category;
    }

    public Double getTotalSales() {
        return totalSales;
    }

    public void setTransactionDate(Date transactionDate) {
        this.transactionDate = transactionDate;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public void setTotalSales(Double totalSales) {
        this.totalSales = totalSales;
    }
}
