package com.ptasdevz;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableResult;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class AzureTableStorage {

    //Table Storage Connect String
    private  String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=cloudassignment34ed0;" +
            "AccountKey=U1NMHbxFuE8vFnUFKKTsR/DY0FYZGVuiUDqWKYfM0YCQfh7nMa3jMgeVyur52fyqq0L4hageFuZu9hatbAUQ5w==;" +
            "EndpointSuffix=core.windows.net";

    private  CloudStorageAccount account;
    private  CloudTableClient tableClient;
    private ProductEntityPool productEntityPool;
    private String productTable = "productentity";
    private String failStore = "failstore";
    private static  AzureTableStorage azureTableStorage = new AzureTableStorage();

    private AzureTableStorage(){

        try {
            account = CloudStorageAccount.parse(storageConnectionString);
            productEntityPool = new ProductEntityPool();
            tableClient = account.createCloudTableClient();

        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }

    }

    public  void createTableIfNotExist(String tableName) throws URISyntaxException, StorageException {
        CloudTable cloudTable = tableClient.getTableReference(tableName);
        cloudTable.createIfNotExists();
    }

    public void insertProductEntity(String TransactionId, String UserId, String SellerId, String ProductName, String SalePrice,
                                    String TransactionDate) throws URISyntaxException, StorageException {

        boolean isTableExist = isTableExist(productTable);
        if (isTableExist) {
            ProductEntity productEntity = (ProductEntity) productEntityPool.checkOut();
            productEntity.setTransactionID(TransactionId);
            productEntity.setUserId(UserId);
            productEntity.setProductName(ProductName);
            productEntity.setSellerID(SellerId);
            productEntity.setSalePrice(SalePrice);
            productEntity.setTransactionDate(TransactionDate);

            CloudTable cloudTable = tableClient.getTableReference(productTable);
            TableOperation insertProduct = TableOperation.insertOrReplace(productEntity);
            cloudTable.execute(insertProduct);
        }
        else {
            this.createTableIfNotExist(productTable);
        }

    }

    public void insertProductEntity(ProductEntity productEntity) throws URISyntaxException, StorageException {

        boolean isTableExist = isTableExist(productTable);
        if (isTableExist) {

            CloudTable cloudTable = tableClient.getTableReference(productTable);
            TableOperation insertProduct = TableOperation.insertOrReplace(productEntity);
            cloudTable.execute(insertProduct);
        }
        else {
            this.createTableIfNotExist(productTable);
            insertProductEntity(productEntity);
        }

    }

    public void insertFailStoreEntity(ProductEntity productEntity) throws URISyntaxException, StorageException {

        boolean isTableExist = isTableExist(failStore);
        if (isTableExist) {

            CloudTable cloudTable = tableClient.getTableReference(failStore);
            TableOperation insertProduct = TableOperation.insertOrReplace(productEntity);
            cloudTable.execute(insertProduct);
        }
        else {
            this.createTableIfNotExist(failStore);
            insertFailStoreEntity(productEntity);
        }

    }


    private boolean isTableExist(String tableName) {

        for (String table : tableClient.listTables()) {
            if (table.equalsIgnoreCase(tableName)) return true;
        }
        return false;
    }

    public static AzureTableStorage getInstance(){
        return azureTableStorage;
    }



}
