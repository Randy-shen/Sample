require 'fakes3'

class FakeS3Server
  def initialize(pid)
    @pid = pid
  end

  def self.up
    fakes3_path = File.join(File.dirname(__FILE__), '../fakes3/')

    pid = spawn("bundle exec fakes3 --port 10001 --root #{fakes3_path} -q")

    @@instance = FakeS3Server.new(pid)

    @@instance
  end

  def self.down
    @@instance.down if defined? @@instance
  end

  def down
    return unless @pid

    Process.kill('SIGINT', @pid)
    Process.waitpid2(@pid)
    @pid = nil
  end
end
