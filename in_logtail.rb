class LogTaiInput < Fluent::TailInput
		Fluent::Plugin.register_input('logtail', self)

		def initialize
				super
		end

		def configure(conf)
				super
		end

		def parse_line(line)
				format = /^(?<id>[^ ]+)\s+(?<date>\d+)\s+(?<ad_id>[^ ]+)\s(?<pub_id>[^ ]+)\s(?<value>.*)$/
				parser = Fluent::TextParser::RegexpParser.new(format)
				record = parser.call(line)
				return record
		end
end
