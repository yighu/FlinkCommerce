package Dto;

import java.io.Serializable;
import java.sql.Date;


public class SalesPerDay implements Serializable {
    private Date transactionDate;
    private Double totalSales ;

    public SalesPerDay(Date transactionDate, Double totalSales) {
        this.transactionDate = transactionDate;
        this.totalSales = totalSales;
    }

    public Date getTransactionDate() {
        return transactionDate;
    }

    public Double getTotalSales() {
        return totalSales;
    }

    public void setTransactionDate(Date transactionDate) {
        this.transactionDate = transactionDate;
    }

    public void setTotalSales(Double totalSales) {
        this.totalSales = totalSales;
    }
}
