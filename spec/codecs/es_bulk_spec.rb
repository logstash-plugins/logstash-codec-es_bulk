require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/es_bulk"
require "logstash/event"
require "insist"

describe LogStash::Codecs::ESBulk do
  context "#decode" do
    let(:config) { {} }
    let(:subject) { LogStash::Codecs::ESBulk.new(config) }
    it "should return 5 events from json data" do
      data = <<-HERE
      { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }
      { "field1" : "value1" }
      { "delete" : { "_index" : "test", "_type" : "type1", "_id" : "2" } }
      { "create" : { "_index" : "test", "_type" : "type1", "_id" : "3" } }
      { "field1" : "value3" }
      { "update" : {"_id" : "1", "_type" : "type1", "_index" : "index1"} }
      { "doc" : {"field2" : "value2"} }
      { "update" : {"_id" : "5", "_type" : "type1", "_index" : "index1"} }
      { "upsert": {}, "params" : {"field2" : "value2"}, "scripted_upsert": true, "script": "test_script" }
      HERE

      count = 0
      subject.decode(data) do |event|
        case count
        when 0
          insist { event.get("[@metadata][_id]") } == "1"
          insist { event.get("[@metadata][action]") } == "index"
          insist { event.get("field1") } == "value1"
        when 1
          insist { event.get("[@metadata][_id]") } == "2"
          insist { event.get("[@metadata][action]") } == "delete"
        when 2
          insist { event.get("[@metadata][_id]") } == "3"
          insist { event.get("[@metadata][action]") } == "create"
          insist { event.get("field1") } == "value3"
        when 3
          insist { event.get("[@metadata][_id]") } == "1"
          insist { event.get("[@metadata][action]") } == "update"
          insist { event.get("[doc][field2]") } == "value2"
        when 3
          insist { event.get("[@metadata][_id]") } == "5"
          insist { event.get("[@metadata][action]") } == "update"
          insist { event.get("[upsert]") } == {}
          insist { event.get("[params][field2]") } == "value2"
          insist { event.get("[scripted_upsert]") } == true
          insist { event.get("[script]") } == "test_script"
        end
        count += 1
      end
      insist { count } == 5
    end

    it "fail to process non-bulk event then continue" do
      decoded = false
      subject.decode("something that isn't a bulk event\n") do |event|
        decoded = true
      end
      insist { decoded } == false
    end

    it "when parse_update=true, should return 7 events from json data" do
      config.update("parse_update" => true)

      data = <<-HERE
      { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }
      { "field1" : "value1" }
      { "delete" : { "_index" : "test", "_type" : "type1", "_id" : "2" } }
      { "create" : { "_index" : "test", "_type" : "type1", "_id" : "3" } }
      { "field1" : "value3" }
      { "update" : {"_id" : "1", "_type" : "type1", "_index" : "index1"} }
      { "doc" : {"field2" : "value2"} }
      { "update" : {"_id" : "1", "_type" : "type1", "_index" : "index1"} }
      { "doc" : {"field2" : "value2"}, "doc_as_upsert": true }
      { "update" : {"_id" : "5", "_type" : "type1", "_index" : "index1"} }
      { "params" : {"field1" : "value5"}, "script": "some script", "upsert": {} }
      { "update" : {"_id" : "6", "_type" : "type1", "_index" : "index1"} }
      { "params" : {"field1" : "value6"}, "script_id": "some_script_id", "lang": "js", "upsert": { "field2": "value7"} }
      HERE

      count = 0
      subject.decode(data) do |event|
        case count
        when 0
          insist { event.get("[@metadata][_id]") } == "1"
          insist { event.get("[@metadata][action]") } == "index"
          insist { event.get("field1") } == "value1"
        when 1
          insist { event.get("[@metadata][_id]") } == "2"
          insist { event.get("[@metadata][action]") } == "delete"
        when 2
          insist { event.get("[@metadata][_id]") } == "3"
          insist { event.get("[@metadata][action]") } == "create"
          insist { event.get("field1") } == "value3"
        when 3
          insist { event.get("[@metadata][_id]") } == "1"
          insist { event.get("[@metadata][action]") } == "update"
          insist { event.get("[field2]") } == "value2"
        when 4
          insist { event.get("[@metadata][doc_as_upsert]") } == true
          insist { event.get("[field2]") } == "value2"
        when 5
          insist { event.get("[@metadata][_id]") } == "5"
          insist { event.get("[@metadata][action]") } == "update"
          insist { event.get("[@metadata][script]") } == "some script"
          insist { event.get("[@metadata][script_type]") } == "inline"
          insist { event.get("[@metadata][scripted_upsert]") } == true
          insist { event.get("[@metadata][upsert]") } == "{}"
          insist { event.get("[field1]") } == "value5"
        when 6
          insist { event.get("[@metadata][_id]") } == "6"
          insist { event.get("[@metadata][action]") } == "update"
          insist { event.get("[@metadata][script]") } == "some_script_id"
          insist { event.get("[@metadata][script_type]") } == "indexed"
          insist { event.get("[@metadata][script_lang]") } == "js"
          insist { event.get("[@metadata][scripted_upsert]") } == true
          insist { event.get("[@metadata][upsert]") } == "{\"field2\":\"value7\"}"
          insist { event.get("[field1]") } == "value6"
        end
        count += 1
      end
      insist { count } == 7
    end

  end

end
