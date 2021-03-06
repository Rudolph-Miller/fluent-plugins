module Fluent
  class Dynamo < BufferedOutput
    attr_reader :host, :port, :kpi_items

    def initialize
      super
      require "aws-sdk"
      require "msgpack"
      require "time"
      require "json"
    end

    def counfigure (conf)
      @host = couf.has_key?('host') ? conf['host'] : 'localhost'
      @port = conf.has_key?('port') ? conf['port'].to_i : 6379
    end

    def format(tag, time, record)
      convert(record).to_msgpack
    end

  end

  class DynamoOutput < Dynamo
    def start
      super
      aws = []
      open("").each do |line|
        aws.push(line.split(" ").last)
      end

      AWS.config({
        :access_key_id => aws[0],
        :secret_access_key => aws[1],
        :dynamo_db_endpoint => "dynamodb.ap-northeast-1.amazonaws.com"
      })
      tracking = AWS::DynamoDB.new.tables['sometracking']
      tracking.hash_key = [:id, :string]
      tracking.range_key = [:date, :number]
      @items = tracking.items
    end

    def write (chunk)
      chunk.msgpack_each do |record|
        if @items[record['id'], record['time']].exists?
          @items[record['id'], record['time']].attributes.update do |u|
            u.add('value' => 1)
          end
        else
          @items.create(:id => record['id'],
                        :date => record['time'],
                        'ad_id' => record['campaign_token'],
                        'pub_id' => record['campaign_token'],
                        'value' => 1)
        end
      end
    end
  end

  class DynamoOutputUnique < Dynamo
		def start
			super
			aws = []
			open("/Users/tomoya/.aws_credential").each do |line|
				aws.push(line.split(" ").last)
			end

			AWS.config({
				:access_key_id => aws[0],
				:secret_access_key => aws[1],
				:dynamo_db_endpoint => "dynamodb.ap-northeast-1.amazonaws.com"
			})
			tracking = AWS::DynamoDB.new.tables['sometracking']
			tracking.hash_key = [:id, :string]
			tracking.range_key = [:date, :number]
			@items = tracking.items
			unique = AWS::DynamoDB.new.tables['unique_user']
			unique.hash_key = [:id, :string]
			unique.range_key = [:date, :number]
			@uniq = unique.items
		end

		def write (chunk)
			chunk.msgpack_each do |record|
				unless @uniq[record['id'], record['time']].exists?
					@items[record['id'], record['time']].attributes.update do |u|
						u.add('value' => 1)
					end
					@uniq.create(:id => record['id'],
											 :date => record['time'],
											 'ad_id' => record['campaign_token'],
											 'pub_id' => record['campaign_token'],
											 'user'=> record['user'])
				end
			end
		end
  end

  class DynamoImpMonthOutput < DynamoOutput
    Fluent::Plugin.register_output('dynamo-imp-month', self)
    attr_reader :host, :port, :kpi_items

    def convert (hash)
      r = hash['message']
      record = {}
      r.split("\t").each do |item|
        kv = item.split(":", 2)
        key = kv[0]
        val = kv[1]
        record[key] = val
      end
      t = Time.at(record['time'].to_i)
      result={}
      result['time'] = t.strftime("%Y%m").to_i
      result['campaign_token'] = record['campaign_token']
      result['publisher_token'] = record['publisher_token']
      result['id']='imp-'+record['campaign_token']+'-'+record['publisher_token']
      result
    end
  end

  class DynamoImpDayOutput < DynamoOutput
    Fluent::Plugin.register_output('dynamo-imp-day', self)
    attr_reader :host, :port, :kpi_items

    def convert (hash)
      r = hash['message']
      record = {}
      r.split("\t").each do |item|
        kv = item.split(":", 2)
        key = kv[0]
        val = kv[1]
        record[key] = val
      end
      t = Time.at(record['time'].to_i)
      result={}
      result['time'] = t.strftime("%Y%m%d").to_i
      result['campaign_token'] = record['campaign_token']
      result['publisher_token'] = record['publisher_token']
      result['id']='imp-'+record['campaign_token']+'-'+record['publisher_token']

      result
    end

  end

  class DynamoImpMinuteOutput < DynamoOutput
    Fluent::Plugin.register_output('dynamo-imp-minute', self)
    attr_reader :host, :port, :kpi_items

    def convert (hash)
      r = hash['message']
      record = {}
      r.split("\t").each do |item|
        kv = item.split(":", 2)
        key = kv[0]
        val = kv[1]
        record[key] = val
      end
      t = Time.at(record['time'].to_i)
      result={}
      result['time'] = t.strftime("%Y%m%d%H%M").to_i
      result['campaign_token'] = record['campaign_token']
      result['publisher_token'] = record['publisher_token']
      result['id']='imp-'+record['campaign_token']+'-'+record['publisher_token']
      result
    end
  end

  class DynamoUniqueDayOutput < DynamoOutputUnique
    Fluent::Plugin.register_output('dynamo-day-unique', self)
    attr_reader :host, :port, :kpi_items

    def convert (hash)
      r = hash['message']
      record = {}
      r.split("\t").each do |item|
        kv = item.split(":", 2)
        key = kv[0]
        val = kv[1]
        record[key] = val
      end
      t = Time.at(record['time'].to_i)
      result={}
      result['time'] = t.strftime("%Y%m%d").to_i
      result['campaign_token'] = record['campaign_token']
      result['publisher_token'] = record['publisher_token']
      result['id']='imp-'+record['campaign_token']+'-'+record['publisher_token']
			result['user'] = record['user_token']

      result
    end
  end

  class DynamoUniqueMonthOutput < DynamoOutputUnique
    Fluent::Plugin.register_output('dynamo-month-unique', self)
    attr_reader :host, :port, :kpi_items

    def convert (hash)
      r = hash['message']
      record = {}
      r.split("\t").each do |item|
        kv = item.split(":", 2)
        key = kv[0]
        val = kv[1]
        record[key] = val
      end
      t = Time.at(record['time'].to_i)
      result={}
      result['time'] = t.strftime("%Y%m").to_i
      result['campaign_token'] = record['campaign_token']
      result['publisher_token'] = record['publisher_token']
      result['id']='imp-'+record['campaign_token']+'-'+record['publisher_token']
			result['user'] = record['user_token']

      result
    end
  end

  class DynamoRawOutput < DynamoOutput
    Fluent::Plugin.register_output('dynamo-raw', self)
    attr_reader :host, :port, :kpi_items

    def convert (hash)
      r = hash['message']
      record = {}
      r.split("\t").each do |item|
        kv = item.split(":", 2)
        key = kv[0]
        val = kv[1]
        record[key] = val
      end
      t = Time.at(record['time'].to_i)
      result={}
      result['time'] = t.strftime("%Y%m%d%H%M%S%L").to_i
      result['campaign_token'] = record['campaign_token']
      result['publisher_token'] = record['publisher_token']
      result['id']=record['campaign_token']+'-'+record['publisher_token']

      result
    end

  end
end
