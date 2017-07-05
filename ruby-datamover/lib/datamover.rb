require "datamover/version"
require "datamover/datasource"
require "datamover/gpg"
require "datamover/gzip"
require 'datamover/convert_file'

require 'csv'
require 'tempfile'
require 'fileutils'

module TC
  class Datamover
    include GPG
    include Gzip
    include ConvertFile

    attr_accessor :stream_source, :stream_delimiter, :stream_gpg_decrypt,
                  :stream_target, :transformer

    attr_reader :datamover_logs, :source, :target

    def self.logger
      @@logger ||= defined?(Rails) ? Rails.logger : Logger.new(STDOUT)
    end

    def self.logger=(logger)
      @@logger = logger
    end

    def initialize(**options)
      @logging =  options[:logging]
      @verbose = options[:verbose]
      @temp_path = ENV['datamover_temp_path']
      @gpg_key_path = ENV['datamover_gpg_key_path']
      @datamover_logs = []

      self.transformer = TC::Datamover::Transformer::Csv.new
    end

    def from_stream(datasource, **opts)
      self.tap do |datamover|
        datamover.stream_source = datasource
        datamover.stream_delimiter = opts[:delimiter]
        datamover.stream_gpg_decrypt = false
      end
    end

    def transform(transformer)
      self.tap do |datamover|
        datamover.transformer = transformer
      end
    end

    def to_stream(target, **opts)
      self.tap do |datamover|
        datamover.stream_target = target

        ## set the transformer
        stream_source.transformer = transformer if stream_source.respond_to?(:transformer)
        target.transformer = transformer if target.respond_to?(:transformer)
        
        ## initiate the streaming
        target.stream_upload(stream_source)
      end
    end


    def from(source, **options)
      @source = source
      @source_delimiter = options[:delimiter]
      @gpg_decrypt = options[:gpg_decrypt]

      self
    end

    def to(target, **options)
      response_target = options[:response_target]
      compression = options[:compression]
      gpg_encrypt = options[:gpg_encrypt]
      target_delimiter = options[:delimiter]
      response_target = target.get_filename ? target.get_filename : @source.get_filename

      if @verbose == 1
        options[:verbose] = 1
      end

      if (target.class.to_s == 'RedshiftDatasource')
        if (@source.class.to_s != 'S3Datasource')
          s3_staging = Datasource.s3('armatron-ruby').set_remote_directory(File.join('tmp/to_redshift',@source.get_remote_directory,Time.now.strftime("%Y/%m/%d/%H/%M"))).set_filename(@source.get_filename)
          @source = self.from(@source,delimiter:@source_delimiter).to(s3_staging, {delimiter:target_delimiter,compression:'gzip'})
          @datamover_logs += @source.datamover_logs
          @target = target.upload(s3url: @source.target.get_display_name, access_key_id:@source.target.get_access_key_id, secret_access_key:@source.target.get_secret_access_key, **options)
        else
          @target = target.upload(s3url: @source.get_display_name, access_key_id:@source.get_access_key_id, secret_access_key:@source.get_secret_access_key, **options)
        end
      elsif
        data = @source.download(**options)
        @datamover_logs += @source.logs

        if @gpg_decrypt
          puts "@gpg_decrypt detected, decrypting"
          @datamover_logs.push("@gpg_decrypt detected, decrypting")
          data = self.gpg_decrypt(buffer:data, **options)
          response_target = File.basename(response_target,File.extname(response_target))
        end

        if (File.extname(@source.get_filename) == '.gz')
          puts "uncompressing gzip"
          @datamover_logs.push("uncompressing gzip")
          data = self.gzip_uncompress(buffer:data)
          response_target = File.basename(response_target,File.extname(response_target))
        end

        #if source delimiter is provided, use it to convert the file to comma seperated
        if @source_delimiter
          puts "if source delimiter is provided, use it to convert the file to comma seperated"
          @datamover_logs.push("if source delimiter is provided, use it to convert the file to comma seperated")
          data = self.convert(source_delimiter:@source_delimiter,buffer:data)
          response_target = File.basename(response_target,File.extname(response_target))
          response_target << case target_delimiter
              when "\t"
                  ".tsv"
              when ','
                  ".csv"
              else
                  ".txt"
              end
        end

        if (target.class.to_s == 'PgDatasource')
          file = Tempfile.open('datamover', @temp_path) do |f|
            f << data
          end
          @target = target.upload(file:file, **options)
        else
          if (compression == 'gzip' && File.extname(@source.get_filename) != '.gz')
            puts "compression detected, gziping"
            @datamover_logs.push("compression detected, gziping")
            data = self.gzip_compress(buffer:data)
            response_target << '.gz'
          end

          if gpg_encrypt
            puts "gpg_encrypt detected, encrypting"
            @datamover_logs.push("gpg_encrypt detected, encrypting")
            data = self.gpg_encrypt(buffer:data, **options)
            response_target << '.gpg'
          end

          @target = target.upload(buffer:data, response_target:response_target, **options)
        end
      end

      self
    ensure
      if @target
        @datamover_logs += @target.logs
        @datamover_logs.uniq!
      end
    end
  end
end
