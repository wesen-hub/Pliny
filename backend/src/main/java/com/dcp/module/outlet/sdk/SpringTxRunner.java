package com.dcp.module.outlet.sdk;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.function.Supplier;

/** 基于 Spring 事务管理器的事务执行器(消费方业务库与位点表同库时使用)。 */
public class SpringTxRunner implements TxRunner {

    private final TransactionTemplate template;

    public SpringTxRunner(PlatformTransactionManager txManager) {
        this.template = new TransactionTemplate(txManager);
    }

    @Override
    public <T> T inTx(Supplier<T> work) {
        return template.execute(status -> work.get());
    }
}
