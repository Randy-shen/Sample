# frozen_string_literal: true

module TC
  class Datamover
    module Datasource
      module Shared
        module Logger
          def logger
            TC::Datamover.logger
          end
        end
      end
    end
  end
end
