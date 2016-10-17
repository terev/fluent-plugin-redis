require 'fluent/output'
require 'digest/sha1'

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

    private
    class Script
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
              [script, Script.new(script.to_s)]
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

    class RedisDumpAggregated < RedisDump
      def initialize(opts, data_type, expiry, aggregate_operator)
        super(opts, data_type, expiry)

        @aggregate_operator = aggregate_operator
        @aggregate_proc = create_aggregate_proc(data_type)
      end

      def write(chunk)
        aggregated = @aggregate_proc.call chunk

        @data_write_proc.call aggregated
      end

      private
      def create_aggregate_proc(data_type)
        case data_type
          when :key_value
            Proc.new do |chunk|
              aggregate_kv(chunk)
            end
          when :hash_map
            Proc.new do |chunk|
              aggregate_hm(chunk)
            end
          else
            raise ConfigError, 'Invalid data type supplied'
        end
      end

      def aggregate_kv(chunk)
        aggregate = {}

        chunk.msgpack_each do |tag, time, record|
          next unless record.is_a?(Hash)

          record.each do |key, value|
            if aggregate.key?(key)
              aggregate[key] = aggregate[key].send(@aggregate_operator, value)
            else
              aggregate[key] = value
            end
          end
        end

        aggregate
      end

      def aggregate_hm(chunk)
        aggregate = {}

        chunk.msgpack_each do |tag, time, record|
          next unless record.is_a?(Hash)

          record.each do |key, value|
            next unless value.is_a?(Hash)

            aggregate[key] = {} unless aggregate.key?(key)

            aggregate[key].merge!(value) { |k, v1, v2|
              if v1 && v1.is_a?(Numeric) && v2.is_a?(Numeric)
                v1.send(@aggregate_operator, v2)
              else
                v2
              end
            }
          end
        end
        aggregate
      end
    end
  end
end
