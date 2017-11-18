package com.ptasdevz;

import com.microsoft.azure.storage.table.TableServiceEntity;

public  class ProductEntity extends TableServiceEntity {

    private String TransactionID;
    private String UserId;
    private String SellerID;
    private String ProductName;
    private String SalePrice;
    private String TransactionDate;


    public String getTransactionID() {
        return TransactionID;
    }

    public void setTransactionID(String transactionID) {
        this.rowKey = transactionID;
        TransactionID = transactionID;
    }

    public String getUserId() {
        return UserId;
    }

    public void setUserId(String userId) {
        this.partitionKey = userId;
        UserId = userId;
    }

    public String getSellerID() {
        return SellerID;
    }

    public void setSellerID(String sellerID) {
        SellerID = sellerID;
    }

    public String getProductName() {
        return ProductName;
    }

    public void setProductName(String productName) {
        ProductName = productName;
    }

    public String getSalePrice() {
        return SalePrice;
    }

    public void setSalePrice(String salePrice) {
        SalePrice = salePrice;
    }

    public String getTransactionDate() {
        return TransactionDate;
    }

    public void setTransactionDate(String transactionDate) {
        TransactionDate = transactionDate;
    }

    @Override
    public String toString() {
        return "ProductEntity{" +
                "TransactionID='" + TransactionID + '\'' +
                ", UserId='" + UserId + '\'' +
                ", SellerID='" + SellerID + '\'' +
                ", ProductName='" + ProductName + '\'' +
                ", SalePrice='" + SalePrice + '\'' +
                ", TransactionDate='" + TransactionDate + '\'' +
                '}';
    }
}