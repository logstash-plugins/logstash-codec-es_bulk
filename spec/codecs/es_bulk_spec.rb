require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/es_bulk"
require "logstash/event"
require "insist"
require 'logstash/plugin_mixins/ecs_compatibility_support/spec_helper'

describe LogStash::Codecs::ESBulk, :ecs_compatibility_support  do
  ecs_compatibility_matrix(:disabled, :v1, :v8) do |ecs_select|
    before(:each) do
      allow_any_instance_of(described_class).to receive(:ecs_compatibility).and_return(ecs_compatibility)
    end

    subject do
      next LogStash::Codecs::ESBulk.new
    end

    context "#decode" do
      it "should return 4 events from json data" do
        data = <<-HERE
      { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }
      { "field1" : "value1" }
      { "delete" : { "_index" : "test", "_type" : "type1", "_id" : "2" } }
      { "create" : { "_index" : "test", "_type" : "type1", "_id" : "3" } }
      { "field1" : "value3" }
      { "update" : {"_id" : "1", "_type" : "type1", "_index" : "index1"} }
      { "doc" : {"field2" : "value2"} }
        HERE

        metadata_field = '[@metadata][codec][es_bulk]'

        count = 0
        subject.decode(data) do |event|
          case count
          when 0
            insist { event.get("#{metadata_field}[_id]") } == "1"
            insist { event.get("#{metadata_field}[action]") } == "index"
            insist { event.get("field1") } == "value1"
          when 1
            insist { event.get("#{metadata_field}[_id]") } == "2"
            insist { event.get("#{metadata_field}[action]") } == "delete"
          when 2
            insist { event.get("#{metadata_field}[_id]") } == "3"
            insist { event.get("#{metadata_field}[action]") } == "create"
            insist { event.get("field1") } == "value3"
          when 3
            insist { event.get("#{metadata_field}[_id]") } == "1"
            insist { event.get("#{metadata_field}[action]") } == "update"
            insist { event.get("[doc][field2]") } == "value2"
          end
          count += 1
        end
        insist { count } == 4
      end
    end if ecs_select.active_mode != :disabled

    context "fail to process non-bulk event then continue" do
      it "continues after a fail" do
        decoded = false
        subject.decode("something that isn't a bulk event\n") do |event|
          decoded = true
        end
        insist { decoded } == false
      end
    end

  end

end
