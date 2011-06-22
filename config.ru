require 'rack/rewrite'
use Rack::Rewrite do
  rewrite %r{(.*)/}, '$1/index.html'
end
run Rack::File.new('public')
