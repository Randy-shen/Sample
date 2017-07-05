require 'csv'

module ConvertFile

    def convert(source_delimiter:",", target_delimiter:",", **options)
        local_filename = options[:local_filename]
        buffer = options[:buffer]

        data = local_filename ? File.open(local_filename) : buffer
        if source_delimiter.to_s != target_delimiter.to_s

            unconverted_data = CSV.parse(data, {:col_sep => source_delimiter})

            converted_data = CSV.generate({:col_sep => target_delimiter}) do |csv|
                unconverted_data.each {|row|
                    csv << row
                }
            end

            return converted_data
        else
            return data
        end
    end
end
