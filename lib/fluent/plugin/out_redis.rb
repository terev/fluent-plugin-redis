require 'fluent/output'

module Fluent
  class RedisOutput < BufferedOutput
    Fluent::Plugin.register_output('redis', self)

    config_param :host, :string, :default => 'localhost'
    config_param :socket_path, :string, :default => nil
    config_param :port, :integer, :default => 6379
    config_param :password, :string, :default => nil, :secret => true
    config_param :db_number, :integer, :default => nil
    config_param :data_type, :string, :default => 'key_value'
    config_param :ttl, :integer, :default => nil
    config_param :aggregate_operator, :string, :default => nil


    def initialize
      super
      require 'redis'
      require 'msgpack'
    end

    def configure(conf)
      super

      if @host.nil? && @socket_path.nil?
        raise ConfigError, 'One of host or socket_path must be set to connect to redis server'
      end

      if conf.has_key?('namespace')
        $log.warn "namespace option has been removed from fluent-plugin-redis 0.1.3. Please add or remove the namespace '#{conf['namespace']}' manually."
      end
    end

    def start
      super

      opts = {:thread_safe => true, :db => @db_number}

      opts[:password] = @password unless @password.nil?

      if !@socket_path.nil?
        opts[:path] = @socket_path
      else
        opts[:host] = @host
        opts[:port] = @port
      end

      @redis = Redis.new(opts)
    end

    def shutdown
      @redis.quit
    end

    def format(tag, time, record)
      identifier = [tag, time].join('.')
      [identifier, record].to_msgpack
    end

    def aggregate_chunk(chunk)
      if @data_type == 'key-value'
        aggregate_key_value(chunk)
      end
    end

    def aggregate_key_value(chunk)
      chunk.msgpack_each do |tag, time, record|
        puts record.inspect
      end
    end

    def aggregate_hm

    end

    def write(chunk)
      @redis.pipelined {
        chunk.open { |io|
          begin
            MessagePack::Unpacker.new(io).each.each_with_index { |record, index|
              @redis.mapped_hmset "#{record[0]}.#{index}", record[1]
            }
          rescue EOFError
            # EOFError always occured when reached end of chunk.
          end
        }
      }
    end
  end
end
