#!/usr/bin/env ruby
require 'rubygems'
require 'redis'

junta = Redis::Client.new :port => 8046

exe = `which beanstalkd`.chomp

junta.call "set", "/j/local/mon/def/a.service/service/exec-start", exe, ""
junta.call "set", "/j/local/mon/def/a.service/service/restart", "restart-always", ""
junta.call "set", "/j/local/mon/ctl/a.service", "start", ""
