require 'test_helper'

class RtpTest < ActiveSupport::TestCase
  load_schema

  class User < ActiveRecord::Base
  end
  class Status < ActiveRecord::Base
  end

  def load_statuses(lower_limit=0, upper_limit=228)
    fixtures = YAML::load(IO.read(File.dirname(__FILE__) + '/fixtures/fixtures.yml'))
    records = fixtures['statuses']['records']
    column_names = fixtures['statuses']['columns']
    quoted_column_names = column_names.map { |column| ActiveRecord::Base.connection.quote_column_name(column) }.join(',')
    records.each do |record|
      data = {}
      column_names.size.times { |i| data[ column_names[i] ] = record[i] }
      if data['id'].to_i < lower_limit
        next
      end
      if data['id'].to_i > upper_limit
        break
      end
      ActiveRecord::Base.connection.execute(
      "INSERT INTO statuses (#{quoted_column_names}) VALUES (#{record.map { |r| ActiveRecord::Base.connection.quote(r) }.join(',')})"
      )
    end
  end
  
  def teardown
    Status.destroy_all
  end

  def _test_schema_has_loaded_correctly 
    assert_equal [], User.all
    assert_equal [], Status.all 
  end

  def _test_initial_information_schema
    ctp = RollingTablePartition.current_table_partitions(Status)
    assert_equal 1, ctp.size
    assert ctp[0]['max_part_id'].nil?
    assert ctp[0]['part_name'].nil?
    assert ctp[0]['seq'].nil?
  end
  
  def _test_initial_partitioning
    load_statuses(0, 150)
    assert_equal 150, Status.count
    rtps = RollingTablePartition.initialize_partitions(Status, 10)
    assert_equal 8, rtps.size
    assert_equal 10, PartitionedTable.daily_id_rate(Status, days_back=7, now=Time.parse("Sun Feb 28 00:00:00 UTC 2010").gmtime)
    assert_equal 'statuses_0', rtps[0].part_name
    assert_equal 'statuses_maxvalue', rtps[-1].part_name
    assert_equal 'statuses_6', rtps[-2].part_name
    assert_equal Time.parse("Mon Mar 1 16:26:48 UTC 2010"), rtps[0].max_created_at
  end

  def test_roll_partition
    load_statuses(0, 150)
    assert_equal 150, Status.count
    RollingTablePartition.initialize_partitions(Status, 10)
    assert_equal 150, Status.count
    rtps = RollingTablePartition.roll_partition(Status, now=Time.parse("Mon Mar 8 00:00:00 UTC 2010").gmtime)
    # grow the table by 10
    load_statuses(151, 160)
    assert_equal 160, Status.count
    rtps = RollingTablePartition.roll_partition(Status, now=Time.parse("Tue Mar 9 00:00:00 UTC 2010").gmtime)
    assert_equal 'statuses_1', rtps[0].part_name
    assert_equal 9, rtps.size
    # statuses_0 was a fat partition, by droppping it, we've shrunk the table
    assert_equal 71, Status.count
    # grow the table by 10
    load_statuses(161, 170)
    assert_equal 81, Status.count
    rtps = RollingTablePartition.roll_partition(Status, now=Time.parse("Wed Mar 10 00:00:00 UTC 2010").gmtime)
    # statuses_1 was dropped
    assert_equal 'statuses_2', rtps[0].part_name
    assert_equal 9, rtps.size
    assert_equal 71, Status.count
  end

end

