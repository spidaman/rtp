ActiveRecord::Schema.define(:version => 0) do

  create_table "statuses", :force => true do |t|
    t.integer  "user_id",           :limit => 8,   :null => false
    t.integer  "twitter_status_id", :limit => 8,   :null => false
    t.datetime "created_at",                       :null => false
    t.string   "text",              :limit => 140
    t.string   "source"
    t.float    "lat"
    t.float    "lon"
    t.boolean  "has_geo"
  end

  add_index "statuses", ["user_id"], :name => "index_statuses_on_user_id"

  create_table "users", :force => true do |t|
    t.integer  "twitter_user_id",    :limit => 8,   :null => false
    t.string   "name",               :limit => 63
    t.string   "url",                :limit => 127
    t.integer  "friends_count"
    t.integer  "followers_count"
    t.integer  "statuses_count"
    t.string   "profile_image_url",  :limit => 127
    t.datetime "twitter_created_at"
    t.string   "location",           :limit => 127
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "rolling_table_partitions", :force => true do |t|
    t.integer  "partitioned_table_id", :null => false
    t.string   "part_name",            :limit => 127, :null => false
    t.string   "max_id",               :limit => 20, :null => false
    t.datetime "max_created_at"
    t.timestamps
  end
  add_index :rolling_table_partitions, [:partitioned_table_id, :part_name], :unique => true, :name => :idx_pt_part_name

  create_table "partitioned_tables", :force => true do |t|
    t.string   "name"
    t.integer  "range_size"
    t.integer  "preserve_if_newer_than", :default => 7
    t.datetime "range_size_calculated_at"
    t.timestamps
  end
end