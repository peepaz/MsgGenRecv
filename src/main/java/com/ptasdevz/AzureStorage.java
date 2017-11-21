package com.ptasdevz;

import com.google.gson.Gson;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableOperation;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Iterator;
import java.util.List;

public class AzureStorage {

    //Table Storage Connect String
    private  String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=cloudassignment34ed0;" +
            "AccountKey=U1NMHbxFuE8vFnUFKKTsR/DY0FYZGVuiUDqWKYfM0YCQfh7nMa3jMgeVyur52fyqq0L4hageFuZu9hatbAUQ5w==;" +
            "EndpointSuffix=core.windows.net";

    private  CloudStorageAccount account;
    private  CloudTableClient tableClient;
    private CloudBlobClient blobClient;
    private ProductEntityPool productEntityPool;
    public static final  String productTable = "productentity";
    public static final String failStore = "failstore";
    public static final String productContainer = "productcontainer";
    public static final String failStoreContainer = "failstorecontainer";
    private static AzureStorage azureStorage = new AzureStorage();
    private int productBlobFileNum = 0;
    private int failStoreBlobFileNum = 0;
    private static final Gson GSON = new Gson();

    private AzureStorage(){

        try {
            account = CloudStorageAccount.parse(storageConnectionString);
            productEntityPool = new ProductEntityPool();
            tableClient = account.createCloudTableClient();
            blobClient = account.createCloudBlobClient();

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


    public void insertProductEntityBatch(List<ProductEntity> entities, String table)
            throws URISyntaxException, StorageException {

        boolean isTableExist = isTableExist(table);
        if (isTableExist) {

            CloudTable cloudTable = tableClient.getTableReference(table);
            TableBatchOperation tableEntityBatch = new TableBatchOperation();

            //genreate batach to install while deleting those added to the batch
            int counter = 1;
            Iterator<ProductEntity> i = entities.iterator();
            while (i.hasNext()) {

                ProductEntity productEntity = i.next();
                if (counter % 100 == 0 ) {
                    tableEntityBatch.insert(productEntity);
                    cloudTable.execute(tableEntityBatch);
                }
                else  {
                    tableEntityBatch.insert(productEntity);
                }
                i.remove();
                counter++;
            }

        }
        else {
            this.createTableIfNotExist(table);
            insertProductEntityBatch(entities,table);
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

    public void insertProductBlob(String productList, String type){

        try {

            CloudBlockBlob blob;

            if (type.equalsIgnoreCase(productContainer)) {

                CloudBlobContainer container = blobClient.getContainerReference(productContainer);
                container.createIfNotExists();
                blob = container.getBlockBlobReference("product_file_"+ productBlobFileNum);
                productBlobFileNum++;

            }
            else {
                CloudBlobContainer container = blobClient.getContainerReference(failStoreContainer);
                container.createIfNotExists();
                blob = container.getBlockBlobReference("product_file_error_"+ failStoreBlobFileNum);
                failStoreBlobFileNum++;
            }
            blob.uploadText(productList);


        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private boolean isTableExist(String tableName) {

        for (String table : tableClient.listTables()) {
            if (table.equalsIgnoreCase(tableName)) return true;
        }
        return false;
    }

    private boolean isBlobContainerExist(String blobContainerName) {

        for (CloudBlobContainer container : blobClient.listContainers()) {
            if (container.getName().equalsIgnoreCase(blobContainerName)) return true;
        }
        return false;
    }

    public static AzureStorage getInstance(){
        return azureStorage;
    }



}
