# encoding: utf-8
require "logstash/codecs/base"
require "logstash/codecs/line"
require "logstash/json"

# This codec will decode the http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-bulk.html[Elasticsearch bulk format]
# into individual events, plus metadata into the `@metadata` field.
#
# Encoding is not supported at this time as the Elasticsearch
# output submits Logstash events in bulk format.
class LogStash::Codecs::ESBulk < LogStash::Codecs::Base
  config_name "es_bulk"

  # Parse update action source line (doc, doc_as_upsert)
  config :parse_update, :validate => :boolean, :default => false

  # Parse scripted_upsert update action source line (script, params, upsert). Relevant only when parse_update=true
  config :parse_scripted_upsert, :validate => :boolean, :default => true

  public
  def initialize(params={})
    super(params)
    @lines = LogStash::Codecs::Line.new
    @lines.charset = "UTF-8"
    @state = :initial
    @metadata = Hash.new
  end

  public
  def decode(data)
    @lines.decode(data) do |bulk|
      begin
        line = LogStash::Json.load(bulk.get("message"))
        case @state
        when :metadata
          if @metadata["action"] == 'update' and @parse_update
            if line.has_key?("doc")
              event = LogStash::Event.new(line["doc"])
              if line.has_key?("doc_as_upsert")
                @metadata["doc_as_upsert"] = line["doc_as_upsert"]
              end
            elsif line.has_key?("params") and @parse_scripted_upsert
              event = LogStash::Event.new(line["params"])
              @metadata["scripted_upsert"] = true
              if line.has_key?("script")
                @metadata["script"] = line["script"]
                @metadata["script_type"] = "inline"
              elsif line.has_key?("script_id")
                @metadata["script"] = line["script_id"]
                @metadata["script_type"] = "indexed"
              end
              if line.has_key?("lang")
                @metadata["script_lang"] = line["lang"]
              end
              if line.has_key?("upsert")
                @metadata["upsert"] = LogStash::Json.dump(line["upsert"])
              end
            else
              # fallback to default
              event = LogStash::Event.new(line)
            end
          else
            event = LogStash::Event.new(line)
          end
          event.set("@metadata", @metadata)
          yield event
          @state = :initial
        when :initial
          @metadata = line[line.keys[0]]
          @metadata["action"] = line.keys[0].to_s
          @state = :metadata
          if line.keys[0] == 'delete'
            event = LogStash::Event.new()
            event.set("@metadata", @metadata)
            yield event
            @state = :initial
          end
        end
      rescue LogStash::Json::ParserError => e
        @logger.error("JSON parse failure. ES Bulk messages must in be UTF-8 JSON", :error => e, :data => data)
      end
    end
  end # def decode

end # class LogStash::Codecs::ESBulk
