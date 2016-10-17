# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |s|
  s.name        = 'fluent-plugin-redis'
  s.version     = File.read('VERSION').strip
  s.authors     = ['Yuki Nishijima', 'Trevor Foster']
  s.date        = %q{2016-10-17}
  s.email       = 'mail@yukinishijima.net'
  s.homepage    = 'http://github.com/yuki24/fluent-plugin-redis'
  s.summary     = 'Redis output plugin for Fluent'

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_dependency 'fluentd', '>= 0.10.58', '< 2'
  s.add_dependency 'redis', '>= 3.0.0'
end
