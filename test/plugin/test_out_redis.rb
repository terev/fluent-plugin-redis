require 'fluent/test'
require 'fluent/plugin/out_redis'

class FileOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup

    @time = Time.parse('2011-01-02 13:14:15 UTC').to_i
  end

  CONFIG = %[
      host localhost
      port 6379
      db_number 1
    ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::RedisOutput).configure(conf)
  end

  def test_configure
    driver = create_driver

    assert_equal 'localhost', driver.instance.host
    assert_equal 6379, driver.instance.port
    assert_equal 1, driver.instance.db_number
  end

  def test_format
    driver = create_driver

    driver.emit({'a' => 1}, @time)
    driver.expect_format(["test.#{@time}", {'a' => 1}].to_msgpack)
    driver.run
  end

  def test_write
    driver = create_driver

    driver.emit({'a' => 2}, @time)
    driver.emit({'a' => 3}, @time)
    driver.run

    assert_equal '2', driver.instance.redis.hget("test.#{@time}.0", 'a')
    assert_equal '3', driver.instance.redis.hget("test.#{@time}.1", 'a')
  end
end
