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

      # Convert data type to a symbol
      @data_type =
          case @data_type
            when 'key_value'
              :key_value
            when 'hash_map'
              :hash_map
            else
              raise ConfigError, 'Invalid data type supplied'
          end

      # Make sure the aggregate operation is valid
      unless @aggregate_operator.nil? || %w(+ - / * %).include?(@aggregate_operator)
        raise ConfigError, 'Invalid aggregate operation supplied'
      end

      if @aggregate_operator
        require 'fluent/plugin/redis_dump_aggregate'
      else
        require 'fluent/plugin/redis_dump'
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

      if @aggregate_operator.nil?
        @redis_dumper = RedisDump.new(opts, @data_type, @ttl)
      else
        @redis_dumper = RedisDumpAggregated.new(opts, @data_type, @ttl, @aggregate_operator)
      end
    end

    def shutdown
      @redis_dumper.quit
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      @redis_dumper.write(chunk)
    end
  end
end
