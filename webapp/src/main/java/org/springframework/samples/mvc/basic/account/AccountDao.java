package org.springframework.samples.mvc.basic.account;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author zznate
 */
public class AccountDao {

  private ColumnFamilyTemplate<Long,String> columnFamilyTemplate;
  private Keyspace keyspace;

  public AccountDao(Keyspace keyspace) {
    this.columnFamilyTemplate = new ThriftColumnFamilyTemplate<Long, String>(keyspace,"Accounts",
            LongSerializer.get(), StringSerializer.get());
    this.keyspace = keyspace;
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


  public List<Account> getAccounts() {
   return null;
  }



  class AccountIterator implements Iterable<HColumn<String,ByteBuffer>> {
    private ColumnSliceIterator<Long,String,ByteBuffer> columnSliceIterator;

    public AccountIterator() {
      SliceQuery<Long,String,ByteBuffer> sliceQuery =
              HFactory.createSliceQuery(keyspace, LongSerializer.get(), StringSerializer.get(), ByteBufferSerializer.get());
      sliceQuery.setColumnFamily("Accounts");
      columnSliceIterator = new ColumnSliceIterator<Long,String,ByteBuffer>(sliceQuery,
              ""+Character.MIN_VALUE, ""+Character.MAX_VALUE,false);
    }

    @Override
    public Iterator<HColumn<String,ByteBuffer>> iterator() {
      return columnSliceIterator;
    }


  }

}
