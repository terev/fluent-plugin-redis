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
    bundle exec rake build

After building the gem 
    

## Configuration

    <match redis.**>
      type redis

      host localhost
      port 6379

      # database number is optional.
      db_number 0        # 0 is default
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
