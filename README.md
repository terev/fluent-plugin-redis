# Redis output plugin for Fluent

fluent-plugin-redis is a fluent plugin to output to redis.

## Installation

This fluentd plugin is available as the `fluent-plugin-redis` gem from RubyGems

    gem install fluent-plugin-redis

Or you can install this plugin for td-agent as:

    td-agent-gem install fluent-plugin-redis

To build the plugin yourself and install it, see the section below. You need [bundler](https://bundler.io/) for this.

    git clone https://github.com/terev/fluent-plugin-redis
    cd fluent-plugin-redis
    bundle install
    gem build fluent-plugin-redis.gemspec

After building the gem install using the same method mentioned above


## Parameters

param|value
--------|------
host|database host (default: localhost)
port|database port (default: 6379)
socket_path|if set connection will be made via unix socket (default: nil)
password|redis password (default: nil)
db_number|the database to initially connect to (default: 0)
ttl|sets expiry for each flushed key in seconds (default: nil)
data_type|data type to write to redis (supports: hash_map, key_value)
aggregate_operator|if set data will be aggregated pre flush (supports: +,-,*,/,%)
    
## Configuration

    <match redis.**>
      @type redis

      host localhost
      port 6379

      # database number is optional.
      db_number 0        # 0 is default
      data_type key_value
      
      flush_interval 5s
    </match>


## Contributing to fluent-plugin-redis
 
* Check out the latest master to make sure the feature hasn't been implemented or the bug hasn't been fixed yet
* Check out the issue tracker to make sure someone already hasn't requested it and/or contributed it
* Fork the project
* Start a feature/bugfix branch
* Commit and push until you are happy with your contribution
* Make sure to add tests for it. This is important so I don't break it in a future version unintentionally.
* Please try not to mess with the Rakefile, version, or history. If you want to have your own version, or is otherwise necessary, that is fine, but please isolate to its own commit so I can cherry-pick around it.


## Copyright

Copyright:: Copyright (c) 2011- Yuki Nishijima
License::   Apache License, Version 2.0
