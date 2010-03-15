class PartitionedTable < ActiveRecord::Base

  def PartitionedTable.daily_id_rate(model, days_back=7, now=Time.now)
    date_format = "%Y-%m-%d 00:00:00"
    max_ids = []
    1.upto(days_back) { |d|
      day = now - d.day
      between = "'%s' AND '%s'" % [(day-1).strftime(date_format), day.strftime(date_format)]
      max_ids <<  model.connection.select_value("SELECT max(id) FROM #{model.table_name} WHERE created_at BETWEEN #{between}") 
    }
    max_ids = max_ids.reject { |mi| mi.nil? }.map { |mi| mi.to_i }
    daily_deltas = []
    max_ids.each_cons(2) { |a,b| daily_deltas << a-b }
    approx(daily_deltas.sum/daily_deltas.size)
  end

  def PartitionedTable.approx(num)
    oom = []
    1.upto(30) { |n| oom << n }
    zeros = oom.reject { |n| 10**n > num }.max
    if zeros > 3
      ((num/(10**(zeros-3))).ceil + 1) * (10**(zeros-3))
    else
      num
    end
  end

end
