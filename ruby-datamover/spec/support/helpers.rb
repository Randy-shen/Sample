require 'benchmark'
require 'memory_profiler'

def print_memory_usage
  memory_before = `ps -o rss= -p #{Process.pid}`.to_i
  yield
  memory_after = `ps -o rss= -p #{Process.pid}`.to_i

  puts "Memory: #{((memory_after - memory_before) / 1024.0).round(2)} MB"
end

def print_time_spent
  time = Benchmark.realtime do
    yield
  end

  puts "Time: #{time.round(2)}"
end

def time_and_memory_usage(function_name, file_size)
  memory_before = `ps -o rss= -p #{Process.pid}`.to_i

  time = Benchmark.realtime do
    yield
  end

  memory_after = `ps -o rss= -p #{Process.pid}`.to_i
  
  memory_in_mb = ((memory_after - memory_before) / 1024.0).round(2)
  time_in_seconds = time.round(2)

  [function_name, file_size, memory_in_mb, time_in_seconds]
end
