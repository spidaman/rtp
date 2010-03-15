class CreatePartitionedTables < ActiveRecord::Migration
  def self.up
    create_table :partitioned_tables do |t|
      t.string   "name"
      t.integer  "range_size"
      t.integer  "preserve_if_newer_than", :default => 7
      t.datetime "range_size_calculated_at"
      t.timestamps
    end
  end

  def self.down
    drop_table :partitioned_tables
  end
end