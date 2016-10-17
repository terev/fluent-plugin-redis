require 'fluent/plugin/redis_dump'

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