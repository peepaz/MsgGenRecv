package com.ptasdevz;

public class ProductEntityPool extends ObjectPool {

    private String TransactionId;
    private String UserId;

    public ProductEntityPool(){
        super();
    }
    @Override
    protected ProductEntity create() {
        return new ProductEntity();
    }


    @Override
    public boolean validate(Object o) {
        return o == null;
    }

    @Override
    public void expire(Object o) {
        ProductEntity p = (ProductEntity) o;
        p = null;
    }
}
