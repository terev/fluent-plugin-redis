require 'digest/sha1'

class RedisScript
  def initialize(file)
    @filepath = File.join(File.dirname(__FILE__), "/../../scripts/#{file}.lua")
    @contents = File.read(@filepath)
    @hash = Digest::SHA1.hexdigest(@contents)
  end

  def call(redis, *args)
    begin
      redis.evalsha(@hash, *args)
    rescue => e
      (e.message =~ /NOSCRIPT/) ? redis.eval(@contents, *args) : raise
    end
  end

  def exists(redis)
    redis.script(:exists, @hash)
  end

  def load(redis)
    $log.info "Loading script: #{@filepath}"
    redis.script(:load, @contents)
  end
end