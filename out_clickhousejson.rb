require 'fluent/plugin/output'
require 'fluent/config/error'
require 'net/http'
require 'date'
require 'yajl'

module Fluent
  module Plugin
    class ClickhousejsonOutput < Fluent::Plugin::Output
        Fluent::Plugin.register_output("clickhousejson", self)
        
        class RetryableResponse < StandardError; end

        helpers :compat_parameters

        DEFAULT_TIMEKEY = 60 * 60 * 24

        desc "URI to ClickHouse node"
        config_param :http_uri, :string
        desc "Database to use"
        config_param :database, :string, default: "default"
        desc "Table to use"
        config_param :table, :string
        desc "User of Clickhouse database"
        config_param :user, :string, default: "default"
        desc "Password of Clickhouse database"
        config_param :password, :string, default: ""
        desc "Offset in minutes, could be useful to substract timestamps because of timezones"
        config_param :tz_offset, :integer, default: 0
        desc "Name of internal fluentd time field (if need to use)"
        config_param :datetime_name, :string, default: nil
        desc "Name of internal fluentd tag field (if need to use)"
        config_param :tag_name, :string, default: nil
        desc "Date time precission for convert to DateTime64(*)"
        config_param :datetime_precision, :integer, default: 0
        desc "Drop fields with null value"
        config_param :drop_null_fields, :bool, default: true
        desc "Raise UnrecoverableError when the response is non success, 4xx/5xx"
        config_param :error_response_as_unrecoverable, :bool, default: false
        desc "The list of retryable response code"
        config_param :retryable_response_codes, :array, value_type: :integer, default: [503]
        config_section :buffer do
            config_set_default :@type, "file"
            config_set_default :chunk_keys, ["time"]
            config_set_default :flush_at_shutdown, true
            config_set_default :timekey, DEFAULT_TIMEKEY
        end

        def configure(conf)
            super
            @uri, @uri_params = make_uri(conf)
            @table            = conf["table"]
            @tz_offset        = conf["tz_offset"].to_i
            @datetime_name    = conf["datetime_name"]

            test_connection(conf)
        end

        def multi_workers_ready?
            true
        end

        def test_connection(conf)
            uri = @uri.clone
            uri.query = URI.encode_www_form(@uri_params.merge({"query" => "SHOW TABLES"}))
            begin
        	    res = Net::HTTP.get_response(uri)
            rescue Errno::ECONNREFUSED
        	    raise Fluent::ConfigError, "Couldn't connect to ClickHouse at #{ @uri } - connection refused"
            end
            if res.code != "200"
                raise Fluent::ConfigError, "ClickHouse server responded non-200 code: #{ res.body }"
            end
        end

        def make_uri(conf)
            uri = URI("#{ conf["http_uri"] }/")
            params = {
                "database" => conf["database"] || "default",
                "user"     => conf["user"] || "default",
                "password" => conf["password"] || "",
                "input_format_skip_unknown_fields" => 1
            }
            return uri, params
        end

        def format(tag, timestamp, record)
            if @datetime_name
                if @datetime_precision > 0
                   record[@datetime_name] = (timestamp.to_f * (10**@datetime_precision)).to_i + @tz_offset * 60 
                else
                   record[@datetime_name] = timestamp + @tz_offset * 60
                end
            end

            if @tag_name
                record[@tag_name] = tag
            end

            if @drop_null_fields
                new_record = record.dup
                new_record.each_key do |k|
                    if new_record[k] == nil
                        new_record.delete(k)
                    end
                end
                return Yajl.dump(new_record) + "\n"
            end

            return Yajl.dump(record) + "\n"
	    end

        def write(chunk)
          uri = @uri.clone
          query_table = extract_placeholders(@table, chunk)
          query = {"query" => "INSERT INTO #{query_table} FORMAT JSONEachRow"}
          uri.query = URI.encode_www_form(@uri_params.merge(query))

          req = Net::HTTP::Post.new(uri)
          req.body = chunk.read

          http = Net::HTTP.new(uri.hostname, uri.port)
          if uri.instance_of?  URI::HTTPS
              http.use_ssl = true
              http.verify_mode = OpenSSL::SSL::VERIFY_NONE
          end
          resp = http.request(req)

          if resp.is_a?(Net::HTTPSuccess)
            return
          end
          
          msg = "Clickhouse responded: #{resp.body}"

          if @retryable_response_codes.include?(resp.code.to_i)
            raise RetryableResponse, msg
          end

          if @error_response_as_unrecoverable
            raise Fluent::UnrecoverableError, msg
          else
            log.error msg
          end
        end
    end
  end
end