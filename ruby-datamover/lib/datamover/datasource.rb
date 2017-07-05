require 'erb'
require 'yaml'

require 'datamover/setup'

require 'datamover/datasource/sftp.rb'
require 'datamover/datasource/pg.rb'
require 'datamover/datasource/redshift.rb'
require 'datamover/datasource/s3.rb'
require 'datamover/datasource/sqlserver.rb'
require 'datamover/datasource/ftp_datasource'
require 'datamover/datasource/puts_datasource'
require 'datamover/datasource/file_datasource'

module TC
  module Datasource
    setup = TC::Datasource::Setup.new
    @env = setup.datamover_env
    @DATASOURCES_CONFIG = setup.datasources_config

    def Datasource.get_config(datasource_type:, datasource_name:, env: @env)
      begin
        return @DATASOURCES_CONFIG[datasource_type][datasource_name][env]
      rescue NoMethodError
        raise 'Invalid datasource configuration type.'
        exit!
      end
    end

    def Datasource.sftp(datasource_name, env: @env)
      return SftpDatasource.new(self.get_config(datasource_type:'sftp', datasource_name:datasource_name, env: env))
    end

    def Datasource.s3(datasource_name, env: @env)
      return S3Datasource.new(self.get_config(datasource_type:'s3', datasource_name:datasource_name, env: env))
    end

    def Datasource.redshift(datasource_name, env: @env)
      return RedshiftDatasource.new(self.get_config(datasource_type:'redshift', datasource_name:datasource_name, env: env))
    end

    def Datasource.postgres(datasource_name, env: @env)
      return PgDatasource.new(self.get_config(datasource_type:'postgres', datasource_name:datasource_name, env: env))
    end

    def Datasource.sqlserver(datasource_name, env: @env)
      return SqlServerDatasource.new(self.get_config(datasource_type:'sqlserver', datasource_name:datasource_name, env: env))
    end
  end
end
