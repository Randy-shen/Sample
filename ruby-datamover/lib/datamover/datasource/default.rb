module DefaultDatasource
    def set_filepath(filepath)
        @filename = File.basename(filepath.to_s)

        if filepath.to_s.include? '/'
            @remote_directory = File.dirname(filepath.to_s)
        end
    end

    def get_filepath
        if !@remote_directory.to_s.empty?
            if @filename
                return File.join(@remote_directory,@filename)
            else
                return @remote_directory
            end
        else
            return @filename
        end
    end

    def set_remote_directory(remote_directory)
        @logs.push("Setting remote directory...#{remote_directory}")
        @remote_directory = remote_directory.to_s

        self
    end

    def get_remote_directory
        return @remote_directory.to_s
    end

    def set_filename(filename)
        @logs.push("Setting filename...#{filename.to_s}")
        @filename = filename.to_s

        self
    end

    def get_filename
        return @filename.to_s
    end

    def logs
        @logs || []
    end

    class SourceIsEmpty < StandardError
    end

    class InvalidConfigurationValues < StandardError
    end
end
