package org.springframework.samples.mvc.basic.account;

import java.util.Iterator;
import java.util.List;

/**
 * @author zznate
 */
public class AccountDao {

  public Account get(long id) {

    return null;
  }

  public void save(Account account) {
    // for each property, add a mutation, empty[] for null
    // ez-client for each property
  }

  public void delete(Account account) {
    // cf.delete();
  }

  public List<Account> getAccounts() {
    return null;
  }

  public Iterator iterator(long startOnId, int limit) {
    // build slice query here and pass it in?
    return new AccountIterator();
  }
  
  public void upsertFromEm(Account account) {

  }

  public Account getFromEm(long id) {
    return null;
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
