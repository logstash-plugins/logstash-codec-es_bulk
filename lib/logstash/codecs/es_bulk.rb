# encoding: utf-8
require "logstash/codecs/base"
require "logstash/codecs/line"
require "logstash/json"
require 'logstash/plugin_mixins/ecs_compatibility_support'
require 'logstash/plugin_mixins/ecs_compatibility_support/target_check'
require 'logstash/plugin_mixins/validator_support/field_reference_validation_adapter'
require 'logstash/plugin_mixins/event_support/event_factory_adapter'
require 'logstash/plugin_mixins/event_support/from_json_helper'

# This codec will decode the http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-bulk.html[Elasticsearch bulk format]
# into individual events, plus metadata into the `@metadata` field.
#
# Encoding is not supported at this time as the Elasticsearch
# output submits Logstash events in bulk format.
class LogStash::Codecs::ESBulk < LogStash::Codecs::Base
  config_name "es_bulk"

  include LogStash::PluginMixins::ECSCompatibilitySupport(:disabled, :v1, :v8 => :v1)
  include LogStash::PluginMixins::ECSCompatibilitySupport::TargetCheck

  extend LogStash::PluginMixins::ValidatorSupport::FieldReferenceValidationAdapter

  include LogStash::PluginMixins::EventSupport::EventFactoryAdapter

  # Defines a target field for placing decoded fields.
  # If this setting is omitted, data gets stored at the root (top level) of the event.
  #
  # NOTE: the target is only relevant while decoding data into a new event.
  config :target, :validate => :field_reference

  public
  def initialize(params={})
    super(params)
    @lines = LogStash::Codecs::Line.new
    @lines.charset = "UTF-8"
    @state = :initial
    @metadata = Hash.new
    @metadata_field = ecs_select[disabled: '[@metadata]', v1: '[@metadata][codec][es_bulk]']
  end

  def register
  end

  public
  def decode(data)
    @lines.decode(data) do |bulk|
      begin
        line = LogStash::Json.load(bulk.get("message"))
        case @state
        when :metadata
          event = targeted_event_factory.new_event(line)
          event.set(@metadata_field, @metadata)
          yield event
          @state = :initial
        when :initial
          @metadata = line[line.keys[0]]
          @metadata["action"] = line.keys[0].to_s
          @state = :metadata
          if line.keys[0] == 'delete'
            event = targeted_event_factory.new_event
            event.set(@metadata_field, @metadata)
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
