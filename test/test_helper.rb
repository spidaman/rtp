require 'rubygems'
require 'active_support'
require 'active_support/test_case'
require 'active_record'
require 'active_record/fixtures'

ENV['RAILS_ENV'] = 'test' 
ENV['RAILS_ROOT'] ||= File.dirname(__FILE__) + '/../../../..'

require 'test/unit' 
require File.expand_path(File.join(ENV['RAILS_ROOT'], 'config/environment.rb'))
DB_CONFIG = YAML::load(IO.read(File.dirname(__FILE__) + '/database.yml'))

def load_schema 
  ActiveRecord::Base.logger = Logger.new(File.dirname(__FILE__) + "/debug.log")
  ActiveRecord::Base.establish_connection(DB_CONFIG['test'])
  models_dir = File.join(File.dirname( __FILE__ ), '..', 'lib', 'app', 'models')
  Dir[ models_dir + '/*.rb'].each { |m| require m }
  load(File.dirname(__FILE__) + "/schema.rb")
  require File.dirname(__FILE__) + '/../rails/init.rb'
end

