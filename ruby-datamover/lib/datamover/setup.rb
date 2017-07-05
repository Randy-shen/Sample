module TC
  module Datasource
    class Setup
      def datasources_config
        if yaml_file_exists?
          YAML.load(datasources_raw_config)
        else
          warn(path_warn_message)
          nil
        end
      end

      def datamover_env
        if datamover_env_exists?
          ENV['datamover_env']
        else
          warn(env_warn_message)
          nil
        end
      end

      private

      def datasources_raw_config
        ERB.new(File.read(datamover_datasource_path)).result
      end

      def datamover_datasource_path
        ENV['datamover_datasource_path'].to_s
      end

      def yaml_file_exists?
        File.exist?(datamover_datasource_path)
      end

      def datamover_env_exists?
        !ENV['datamover_env'].to_s.strip.empty?
      end

      def path_warn_message
        'Missing ENV[\'datamover_datasource_path\']'
      end

      def env_warn_message
        'Missing ENV[\'datamover_env\']'
      end
    end
  end
end
