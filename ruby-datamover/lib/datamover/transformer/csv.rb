# frozen_string_literal: true

require 'fastcsv'

module TC
  class Datamover
    module Transformer
      class Csv
        def extract_format(line)
          FastCSV.raw_parse(line) do |transformed_line|
            yield transformed_line
          end
        end

        def load_format(line)
          yield FastCSV.generate_line(line)
        end
      end
    end
  end
end
