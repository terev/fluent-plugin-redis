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

      unless @aggregate_operator.nil? || %w(+ - / * %).include?(@aggregate_operator)
        raise ConfigError, 'Invalid aggregate operation supplied'
      end

      unless %w(key-value hash-map).include?(@data_type)
        raise ConfigError, 'Invalid data type supplied'
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

    def write(chunk)
      if !@aggregate_operator.nil?
        data_chunk = aggregate_chunk(chunk)
      else
      end

      # @redis.pipelined {
      #   chunk.open { |io|
      #     begin
      #       MessagePack::Unpacker.new(io).each.each_with_index { |record, index|
      #         @redis.mapped_hmset "#{record[0]}.#{index}", record[1]
      #       }
      #     rescue EOFError
      #       # EOFError always occured when reached end of chunk.
      #     end
      #   }
      # }
    end

    private
    def aggregate_chunk(chunk)
      case @data_type
        when 'key-value'
          aggregate_key_value(chunk)
        when 'hash-map'
          aggregate_hash_map(chunk)
        else
          raise ConfigError, 'Invalid data type supplied'
      end
    end

    def aggregate_key_value(chunk)
      aggregate = Hash.new(0)

      chunk.msgpack_each do |tag, record|
        next unless record.is_a?(Hash)

        record.each do |key, value|
          aggregate[key] = aggregate[key].send(@aggregate_operator, value)
        end
      end

      aggregate
    end

    def aggregate_hash_map(chunk)
      aggregate = {}

      chunk.msgpack_each do |tag, record|
        next unless record.is_a?(Hash)

        record.each do |key, value|
          next unless value.is_a?(Hash)

          aggregate[key] = Hash.new(0) unless aggregate.key?(key)

          aggregate[key].merge!(value) { |k, v1, v2|
            v1.send(@aggregate_operator, v2)
          }
        end
      end

      puts aggregate.inspect

      aggregate
    end
  end
end
