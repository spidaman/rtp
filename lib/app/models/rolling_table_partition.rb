class RollingTablePartition < ActiveRecord::Base

  def RollingTablePartition.current_table_partitions(model)
    select_sql = "SELECT partition_ordinal_position AS seq, partition_name AS part_name, " +
      "partition_description AS max_part_id FROM information_schema.partitions WHERE " +
      "table_name='%s' and table_schema='%s' ORDER BY seq" % 
      [model.table_name, model.connection.current_database]
    model.connection.select_all(select_sql)
  end

  def RollingTablePartition.get_runway_partitions(model)
    ctp = current_table_partitions(model)
    max_id = model.maximum(:id)
    runway_parts = ctp.reject { |part| 
      part['max_part_id'] == 'MAXVALUE' ||  max_id > part['max_part_id'].to_i 
    }
    runway_parts
  end

  def RollingTablePartition.add_new_partition(model, range_size)
    runway_parts = get_runway_partitions(model)
    if runway_parts.size > 2
      return
    end
    ctp = current_table_partitions(model)
    last_part_suffix = ctp[-2]['part_name'].match(/_(\d+)$/).captures[0].to_i
    new_part = "#{model.table_name}_#{last_part_suffix + 1}"
    new_bound = ctp[-2]['max_part_id'].to_i + range_size
    add_new = "ALTER TABLE #{model.table_name} REORGANIZE PARTITION #{model.table_name}_maxvalue INTO (" +
      "PARTITION #{new_part} VALUES LESS THAN (#{new_bound}), " +
      "PARTITION #{model.table_name}_maxvalue VALUES LESS THAN MAXVALUE)"
    model.connection.execute(add_new)
    model.connection.commit_db_transaction
  end

  def RollingTablePartition.drop_old_partitions(model, preserve_if_newer_than=7, now=Time.now)
    # Now get the max age of the data in each partition, drop the partitions with data
    # that exceeds the maximum age
    ctp = current_table_partitions(model)
    ctp.each do |part|
      if part['part_name'] == "#{model.table_name}_maxvalue"
        next
      end
      rtp = find_by_part_name(part['part_name'])
      if rtp.blank?
        # should be: a new partition that doesn't have a model yet
        next
      end
      if ! rtp.nil? && ((now.to_i - rtp.max_created_at.to_i)/1.day.to_i) < preserve_if_newer_than
        next
      end
      drop_old = "ALTER TABLE #{model.table_name} DROP PARTITION #{rtp.part_name}"
      model.connection.execute(drop_old)
    end
    model.connection.commit_db_transaction
  end

  def RollingTablePartition.roll_partition(model, now=Time.now)
    pt = PartitionedTable.find_by_name(model.table_name)
    add_new_partition(model, pt.range_size)
    drop_old_partitions(model, pt.preserve_if_newer_than, now)
    sync_models_from_table(model)
    find(:all, :conditions => ["partitioned_table_id = ?", pt.id])
  end

  # This should sync the application level knowledge of how data is laid out
  # with the database's metadata about how its laid out
  #
  # Not using ActiveRecord::Base.create(...) or other AR change methods because DDL ops in the session
  # can confuse the connection's tx state:
  # Mysql::Error: SAVEPOINT active_record_1 does not exist: RELEASE SAVEPOINT active_record_1
  # WTF?
  def RollingTablePartition.sync_models_from_table(model)
    ctp = current_table_partitions(model)
    pt_id = PartitionedTable.find_by_name(model.table_name).id
    table_max_id = model.maximum(:id)
    ctp.each_with_index do |tp, idx|
      rtp = find(:first, :conditions => ["partitioned_table_id = ? and part_name = ?", pt_id, tp['part_name']])
      tstamp = Time.now.gmtime.strftime("%Y-%m-%d %T")
      # TODO: this boundary detection needs to be refactored
      if rtp.blank?
        if idx > 0
          mca = get_max_created_at(model, tp['max_part_id'], ctp[idx-1]['max_part_id'])
        else
          mca = get_max_created_at(model, tp['max_part_id'], nil)
        end
        if mca.blank?
          mca = 'NULL'
        end
        create_stmt = "INSERT INTO rolling_table_partitions " +
          "(max_id, updated_at, part_name, partitioned_table_id, max_created_at, created_at) " +
          "VALUES('#{tp['max_part_id']}', '#{tstamp}', '#{tp['part_name']}', '#{pt_id}', '#{mca}', '#{tstamp}')"
        connection.execute(create_stmt)
      else
        if tp['max_part_id'] == 'MAXVALUE' && ! ctp[idx-1]['max_part_id'].nil? && ctp[idx-1]['max_part_id'].to_i > table_max_id
          # we're currently writing to the MAXVALUE partition, this is a problem
          mca = get_max_created_at(model, tp['max_part_id'], ctp[idx-1]['max_part_id'])
        else
          if rtp.max_id.nil?
            if idx > 0
              mca = get_max_created_at(model, tp['max_part_id'], ctp[idx-1]['max_part_id'])
            else
              mca = get_max_created_at(model, tp['max_part_id'], nil)
            end
          else
            if rtp.max_id.to_i < table_max_id
              # this partition is "cold", so the max_id isn't advancing, so just keep the old value
              mca = rtp.max_created_at
            else
              if idx > 0
                mca = get_max_created_at(model, tp['max_part_id'], ctp[idx-1]['max_part_id'])
              else
                mca = get_max_created_at(model, tp['max_part_id'], nil)
              end
            end
          end
        end
        if mca.blank?
          mca = 'NULL'
        end
        update_stmt = "UPDATE rolling_table_partitions SET max_id='#{tp['max_part_id']}', updated_at='#{tstamp}', " +
          "max_created_at='#{mca}' WHERE partitioned_table_id='#{pt_id}' AND part_name='#{tp['part_name']}'"
        connection.execute(update_stmt)
      end
    end
    model_partitions = find(:all, :conditions => ["partitioned_table_id = ?", pt_id])
    mysql_partitions_names = ctp.map { |tp| tp['part_name'] }
    model_partitions.each do |part|
      if ! mysql_partitions_names.include?(part.part_name)
        # the partition has been dropped, destroy the model for it
        connection.execute("DELETE FROM rolling_table_partitions WHERE id = #{part.id}")
      end
    end
    connection.commit_db_transaction
  end

  def RollingTablePartition.initialize_partitions(model, range_size, partitions=7)
    max_id = model.maximum(:id)
    part_boundaries = []
    max_id.step(max_id - (range_size * (partitions-1)), range_size * -1) { |v| part_boundaries << v}
    part_boundaries.reverse!
    idx=-1
    parts = part_boundaries.map { |c| "PARTITION #{model.table_name}_#{idx+=1} VALUES LESS THAN (#{c})" }
    parts << "PARTITION #{model.table_name}_maxvalue VALUES LESS THAN MAXVALUE"
    alter_table = "ALTER TABLE #{model.table_name} PARTITION BY RANGE (id) ("
    alter_table << parts.join(",")
    alter_table << ")"
    model.connection.execute(alter_table)
    pt = PartitionedTable.create(:name => model.table_name, :range_size => range_size, :range_size_calculated_at => Time.now)
    sync_models_from_table(model)
    find(:all, :conditions => ["partitioned_table_id = ?", pt.id])
  end

  def RollingTablePartition.get_max_created_at(model, upper_bound, lower_bound)
    where = _mca_where_clause(upper_bound, lower_bound)
    mca = model.maximum(:created_at, :conditions => [where])
    if mca.blank?
      return nil
    else
      return mca.strftime("%Y-%m-%d %T")
    end
  end

  def RollingTablePartition._mca_where_clause(upper_bound, lower_bound)
    if ! lower_bound.nil? && ! upper_bound.nil? && upper_bound != 'MAXVALUE'
      return  "id >= #{lower_bound} AND id < #{upper_bound}"
    elsif lower_bound.nil? && ! upper_bound.nil?
      return  "id < #{upper_bound}"
    elsif upper_bound == 'MAXVALUE' || (! lower_bound.nil? && upper_bound.nil?)
      return "id >= #{lower_bound}"
    else
      nil
    end
  end

end
