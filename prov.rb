#!/usr/bin/env ruby
require 'rubygems'
require 'redis'

doozer = Redis::Client.new :port => 8046

exe = `which beanstalkd`.chomp

doozer.call "set", "/d/local/mon/def/a.service/service/exec-start", exe, ""
doozer.call "set", "/d/local/mon/def/a.service/service/restart", "restart-always", ""
doozer.call "set", "/d/local/mon/ctl/a.service", "start", ""
