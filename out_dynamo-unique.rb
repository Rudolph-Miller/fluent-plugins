module Fluent
	class DynamoUniqueOutput < BufferedOutput
		Fluent::Plugin.register_output('dynamo-raw', self)
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
			unique = AWS::DynamoDB.new.tables['unique-user']
			unique.hash_key = [:id, :string]
			unique.range_key = [:date, :number]
			@uniq = unique.items
		end

		def shutdown
		end

		def convert (hash)
			r = hash['message']
			record = {}
			r.split("\t").each do |item|
				kv = item.split(":", 2)
				key = kv[0]
				val = kv[1]
				record[key] = val
			end

			acc=[]
			time = record['time']
			len=time.length
			time.slice(1, len-2).split(":").each do |item|
				acc.push(item.split("/"))
			end
			acc.flatten!
			t=Time.mktime(acc[2],acc[1],acc[0],acc[3],acc[4],acc[5],acc[6])

			result={}
			#result['time'] = t.strftime("%Y%m%d%H%M%S%L").to_i
			result['time'] = t.strftime("%Y%m%d").to_i
			#result['time'] = t.strftime("%Y%m").to_i
			result['campaign_token'] = record['campaign_token']
			result['publisher_token'] = record['publisher_token']
			result['id']=record['campaign_token']+'-'+record['publisher_token']
			result['user'] = record['user_token']

			result
		end

		def format(tag, time, record)
			convert(record).to_msgpack
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
											 'user'=> record['user'],
											 'value' => 1)
				end
			end
		end
	end
end


