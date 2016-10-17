require 'fluent/plugin/redis_script'

class RedisDump
  def initialize(redis_opts, data_type, expiry)
    @redis = Redis.new(redis_opts)

    @data_write_proc = create_data_dump_proc(data_type)
    @expiry = expiry if expiry

    load_scripts(data_type)
  end

  def load_scripts(data_type)
    script_list = case data_type
                    when :key_value
                      [:key_value_dump_expire]
                    when :hash_map
                      @expiry ? [:hash_dump_expire] : [:hash_dump]
                    else
                  end

    @scripts = Hash[
        script_list.map do |script|
          [script, RedisScript.new(script.to_s)]
        end]
  end

  def quit
    @redis.quit
  end

  def create_data_dump_proc(data_type)
    case data_type
      when :key_value
        Proc.new do |record|
          write_kv(record)
        end
      when :hash_map
        Proc.new do |record|
          write_hm(record)
        end
      else
        raise ConfigError, 'Invalid data type supplied'
    end
  end

  # @param [Hash] records
  def write_hm(records)
    script = @expiry ? @scripts[:hash_dump_expire] : @scripts[:hash_dump]
    script_exists = script ? script.exists(@redis) : nil

    @redis.pipelined {
      script.load(@redis) if script && !script_exists

      records.each do |key, values|
        next unless values.is_a?(Hash)

        argv = [key]
        argv << @expiry if @expiry

        script.call(@redis, :keys => values.flatten, :argv => argv)
      end
    }
  end

  def write_kv(records)
    script = @expiry ? @scripts[:key_value_dump_expire] : nil
    script_exists = script ? script.exists(@redis) : nil

    @redis.pipelined {
      script.load(@redis) if script && !script_exists

      records.each do |key, value|
        next if value.is_a?(String)

        if script
          script.call(@redis, :keys => [key], :argv => [value, @expiry])
        else
          @redis.incrby(key, value)
        end
      end
    }
  end

  def write(chunk)
    chunk.msgpack_each { |tag, time, record|
      next unless record.is_a?(Hash)

      @data_write_proc.call record
    }
  end
end