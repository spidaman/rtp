class CreateRollingTablePartitions < ActiveRecord::Migration
  def self.up
    create_table :rolling_table_partitions do |t|
      t.integer  "partitioned_table_id", :null => false
      t.string   "part_name",            :limit => 127, :null => false
      t.string   "max_id",               :limit => 20, :null => false
      t.datetime "max_created_at"
      t.timestamps
    end
    add_index :rolling_table_partitions, [:partitioned_table_id, :part_name], :unique => true, :name => :idx_pt_part_name
  end

  def self.down
    drop_table :rolling_table_partitions
  end
end



