package org.springframework.samples.mvc.basic.account;

import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

/**
 * @author zznate
 */
public class AccountDao {

  private ColumnFamilyTemplate<Long,String> columnFamilyTemplate;

  public void setColumnFamilyTemplate(ColumnFamilyTemplate cft) {
    this.columnFamilyTemplate = columnFamilyTemplate;
  }

  public Account get(long id) {
    ColumnFamilyResult<Long,String> result = columnFamilyTemplate.queryColumns(id);
    Account account = new Account();
    account.setId(result.getKey());
    account.setBalance(new BigDecimal(result.getDouble("balance")));
    account.setName(result.getString("name"));
    account.setRenewalDate(result.getDate("renewalDate"));
    return account;
  }

  public void save(Account account) {
    ColumnFamilyUpdater<Long,String> updater = columnFamilyTemplate.createUpdater(account.getId());
    updater.setDouble("balance",account.getBalance().doubleValue());
    updater.setString("name",account.getName());
    updater.setDate("renewalDate",account.getRenewalDate());
    columnFamilyTemplate.update(updater);
  }

  public void delete(Account account) {
    columnFamilyTemplate.deleteRow(account.getId());
  }


  public Iterator iterator(long startOnId, int limit) {
    // build slice query here and pass it in?
    return new AccountIterator();
  }


  class AccountIterator implements Iterator<Account>,Iterable<Account> {

    public AccountIterator() {
      // create columnSliceIterator
    }

    @Override
    public Iterator<Account> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Account next() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void remove() {
      //To change body of implemented methods use File | Settings | File Templates.
    }
  }

}
