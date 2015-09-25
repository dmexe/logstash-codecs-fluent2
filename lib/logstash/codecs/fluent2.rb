# encoding: utf-8
require "logstash/codecs/fluent2/version"

require "logstash/codecs/base"
require "logstash/util/charset"
require "logstash/timestamp"
require "logstash/util"

# This codec handles fluentd's msgpack schema.
#
# For example, you can receive logs from `fluent-logger-ruby` with:
# [source,ruby]
#     input {
#       tcp {
#         codec => fluent
#         port => 4000
#       }
#     }
#
# And from your ruby code in your own application:
# [source,ruby]
#     logger = Fluent::Logger::FluentLogger.new(nil, :host => "example.log", :port => 4000)
#     logger.post("some_tag", { "your" => "data", "here" => "yay!" })
#
# Notes:
#
# * the fluent uses a second-precision time for events, so you will never see
#   subsecond precision on events processed by this codec.

class LogStash::Codecs::Fluent2 < LogStash::Codecs::Base
  config_name "fluent2"

  public
  def register
    require "msgpack"
    @decoder = MessagePack::Unpacker.new
  end

  public

  def debug(str)
    $stdout.puts(str) if ENV['DEBUG']
  end

  def decode(data)
    begin
      debug("[fluent2] feed data=#{data.bytesize} bytes")
      @decoder.feed(data)
      @decoder.each do |tag, _payload|
        debug("[fluent2] entry tag=#{tag} payload=#{_payload.bytesize} bytes")
        epochtime, map = MessagePack.unpack(_payload)
        event = LogStash::Event.new(map.merge(
          LogStash::Event::TIMESTAMP => LogStash::Timestamp.at(epochtime),
          "tags" => tag
        ))
        debug("[fluent2] event=#{map.inspect}")
        yield event
      end
    rescue Exception => e
      $stderr.puts "BROKEN: #{data.inspect}"
      raise e
    end
  end # def decode

  public
  def encode(event)
    tag = event["tags"] || "log"
    epochtime = event.timestamp.to_i

    # use normalize to make sure returned Hash is pure Ruby for
    # MessagePack#pack which relies on pure Ruby object recognition
    data = LogStash::Util.normalize(event.to_hash)
    # timestamp is serialized as a iso8601 string
    # merge to avoid modifying data which could have side effects if multiple outputs
    @on_event.call(event, MessagePack.pack([tag, epochtime, data.merge(LogStash::Event::TIMESTAMP => event.timestamp.to_iso8601)]))
  end # def encode

end # class LogStash::Codecs::Fluent
