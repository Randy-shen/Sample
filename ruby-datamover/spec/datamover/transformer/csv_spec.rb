# frozen_string_literal: true
require 'spec_helper'
require 'tempfile'

describe TC::Datamover::Transformer::Csv do
  let!(:transformer) { described_class.new }

  describe '#load_format' do
    it 'formats the line from the array' do
      line = %w(some line)

      transformer.load_format(line) do |transformed_line|
        expect(transformed_line).to eql("some,line\n")
      end
    end

    it 'formats oddly formed lines - making sure they are parseable when later streamed down' do
      line = ["CA", "KNAFE161 5", 275371, 2005, "2.0L", "4 Cylinder Engine", "Gasoline Fuel", "ST75D5", 22695.0, "w/\"Package 1\""]

      transformer.load_format(line) do |transformed_line|
        expect(transformed_line).to eql("CA,KNAFE161 5,275371,2005,2.0L,4 Cylinder Engine,Gasoline Fuel,ST75D5,22695.0,\"w/\"\"Package 1\"\"\"\n")
      end

      line = ["CA", "KNAHU8A3 F", 371346, 2015, "2.0L", "4 Cylinder Engine", "Gasoline Fuel", "RN758F", 33123.4, "EX Luxury w/Nav/17\" Wheels"]

      transformer.load_format(line) do |transformed_line|
        expect(transformed_line).to eql("CA,KNAHU8A3 F,371346,2015,2.0L,4 Cylinder Engine,Gasoline Fuel,RN758F,33123.4,\"EX Luxury w/Nav/17\"\" Wheels\"\n")
      end
    end 
  end
end
